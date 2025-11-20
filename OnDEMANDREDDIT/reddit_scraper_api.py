#!/usr/bin/env python3
"""
reddit_scraper_api.py

Production-grade Reddit Scraper API Server:
✅ RESTful API with FastAPI
✅ Real-time scraping with response streaming
✅ Account pool management with auto-failover
✅ DataEntity format output
✅ Rate limit handling
✅ Parallel processing
✅ Health monitoring
"""

import os
import sys
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field
import uvicorn

# Import our custom modules
from account_pool_manager import AccountPoolManager
from realtime_scraper import RealtimeScraper

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('scraper_api.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Global account pool manager
account_pool: Optional[AccountPoolManager] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    global account_pool
    
    # Startup
    logger.info("=" * 80)
    logger.info("Reddit Scraper API Starting")
    logger.info("=" * 80)
    
    # Initialize account pool
    account_pool = AccountPoolManager("envActive")
    loaded = await account_pool.load_accounts()
    
    if loaded == 0:
        logger.error("No accounts loaded! Check envActive file")
        raise RuntimeError("No Reddit accounts available")
    
    logger.info(f"✓ Loaded {loaded} Reddit accounts")
    logger.info("✓ API Server ready to accept requests")
    
    yield
    
    # Shutdown
    logger.info("Shutting down API server...")
    await account_pool.cleanup()


# Initialize FastAPI with lifespan
app = FastAPI(
    title="Reddit Scraper API",
    description="Real-time Reddit scraping with DataEntity format",
    version="1.0.0",
    lifespan=lifespan
)


# ============================================
# REQUEST/RESPONSE MODELS
# ============================================

class ScrapeRequest(BaseModel):
    """Scrape request model"""
    subreddits: List[str] = Field(
        ..., 
        description="List of subreddit names (without r/ prefix)",
        example=["python", "machinelearning"]
    )
    max_posts: int = Field(
        default=100,
        ge=1,
        le=1000,
        description="Maximum posts to scrape per subreddit"
    )
    include_comments: bool = Field(
        default=True,
        description="Whether to scrape comments"
    )
    max_comments_per_post: int = Field(
        default=500,
        ge=0,
        le=1000,
        description="Maximum comments per post"
    )
    days_back: int = Field(
        default=7,
        ge=1,
        le=30,
        description="How many days back to scrape"
    )


class SearchRequest(BaseModel):
    """Search request model"""
    subreddits: List[str] = Field(
        ...,
        description="List of subreddit names (without r/ prefix)",
        example=["python", "machinelearning"]
    )
    keywords: List[str] = Field(
        ...,
        min_length=1,
        description="List of keywords to search for in each subreddit"
    )
    max_posts: int = Field(
        default=100,
        ge=1,
        le=1000,
        description="Maximum posts to return per subreddit-keyword pair"
    )
    include_comments: bool = Field(
        default=True,
        description="Whether to fetch comments for matched posts"
    )
    max_comments_per_post: int = Field(
        default=500,
        ge=0,
        le=1000,
        description="Maximum comments per post"
    )
    days_back: int = Field(
        default=7,
        ge=1,
        le=30,
        description="How many days back to search"
    )

class ScrapeStats(BaseModel):
    """Scraping statistics"""
    posts_scraped: int
    comments_scraped: int
    subreddits_processed: int
    duration_seconds: float
    account_used: str
    errors_count: int


class ScrapeResponse(BaseModel):
    """Scrape response model"""
    status: str
    data: List[Dict[str, Any]]
    stats: ScrapeStats
    message: Optional[str] = None


# ============================================
# API ENDPOINTS
# ============================================

async def run_scrape_with_fallback(scraper, subreddits: List[str]) -> Dict[str, Any]:
    """
    Run multi-subreddit scrape using scraper's native method if available,
    otherwise fall back to sequential scrape_subreddit with proper init/cleanup.
    """
    if hasattr(scraper, "scrape_subreddits"):
        return await scraper.scrape_subreddits(subreddits)

    # Fallback path
    await scraper.initialize()
    try:
        all_entities = []
        for name in subreddits:
            ents = await scraper.scrape_subreddit(name)
            all_entities.extend(ents)
        entities_dict = [e.to_json_dict() for e in all_entities]
        return {"data_entities": entities_dict, "stats": scraper.stats}
    finally:
        await scraper.cleanup()

@app.get("/")
async def root():
    """Root endpoint with API info"""
    return {
        "service": "Reddit Scraper API",
        "version": "1.0.0",
        "status": "running",
        "accounts_available": len(account_pool.accounts) if account_pool else 0,
        "accounts_healthy": len(account_pool.get_healthy_accounts()) if account_pool else 0,
        "endpoints": {
            "POST /scrape": "Scrape Reddit subreddits",
            "POST /scrape/auto": "Scrape using all healthy accounts in parallel",
            "POST /search": "Keyword search across subreddits",
            "GET /health": "Health check",
            "GET /accounts": "Account pool status",
            "POST /accounts/reload": "Reload accounts from envActive"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    if not account_pool:
        raise HTTPException(status_code=503, detail="Account pool not initialized")
    
    healthy_count = len(account_pool.get_healthy_accounts())
    total_count = len(account_pool.accounts)
    
    return {
        "status": "healthy" if healthy_count > 0 else "degraded",
        "accounts": {
            "total": total_count,
            "healthy": healthy_count,
            "unhealthy": total_count - healthy_count
        },
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/accounts")
async def get_accounts_status():
    """Get account pool status"""
    if not account_pool:
        raise HTTPException(status_code=503, detail="Account pool not initialized")
    
    return {
        "accounts": [
            {
                "username": acc.username,
                "status": acc.status,
                "requests_made": acc.requests_made,
                "errors": acc.errors,
                "last_used": acc.last_used.isoformat() if acc.last_used else None,
                "rate_limit_remaining": acc.rate_limit_remaining
            }
            for acc in account_pool.accounts
        ],
        "total": len(account_pool.accounts),
        "healthy": len(account_pool.get_healthy_accounts())
    }


@app.post("/accounts/reload")
async def reload_accounts():
    """Reload accounts from envActive file"""
    if not account_pool:
        raise HTTPException(status_code=503, detail="Account pool not initialized")
    
    old_count = len(account_pool.accounts)
    new_count = await account_pool.load_accounts()
    
    return {
        "status": "success",
        "old_count": old_count,
        "new_count": new_count,
        "message": f"Reloaded {new_count} accounts from envActive"
    }


@app.post("/scrape", response_model=ScrapeResponse)
async def scrape_subreddits(request: ScrapeRequest):
    """
    Scrape Reddit subreddits in real-time
    
    Returns DataEntity formatted results
    """
    if not account_pool:
        raise HTTPException(status_code=503, detail="Account pool not initialized")
    
    # Validate request
    if not request.subreddits:
        raise HTTPException(status_code=400, detail="No subreddits provided")
    
    # Get healthy account
    account = account_pool.get_next_account()
    if not account:
        raise HTTPException(
            status_code=503, 
            detail="No healthy accounts available. All accounts may be rate limited or banned."
        )
    
    logger.info(f"[API] Scrape request: {len(request.subreddits)} subreddits using account {account.username}")
    
    try:
        # Create scraper with assigned account
        scraper = RealtimeScraper(
            account=account,
            max_posts=request.max_posts,
            include_comments=request.include_comments,
            max_comments_per_post=request.max_comments_per_post,
            days_back=request.days_back
        )
        
        # Execute scraping
        start_time = datetime.utcnow()
        
        results = await run_scrape_with_fallback(scraper, request.subreddits)
        
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        
        # Build response
        stats = ScrapeStats(
            posts_scraped=results['stats']['posts_scraped'],
            comments_scraped=results['stats']['comments_scraped'],
            subreddits_processed=len(request.subreddits),
            duration_seconds=duration,
            account_used=account.username,
            errors_count=results['stats']['errors']
        )
        
        # Update account stats
        account.requests_made += 1
        account.last_used = datetime.utcnow()
        
        logger.info(
            f"[API] Scrape complete: {stats.posts_scraped} posts, "
            f"{stats.comments_scraped} comments in {duration:.2f}s"
        )
        
        return ScrapeResponse(
            status="success",
            data=results['data_entities'],
            stats=stats,
            message=f"Successfully scraped {stats.posts_scraped} posts from {len(request.subreddits)} subreddit(s)"
        )
        
    except Exception as e:
        logger.error(f"[API] Scrape error: {e}")
        
        # Mark account as potentially problematic
        if "403" in str(e) or "forbidden" in str(e).lower():
            account.mark_forbidden()
            logger.error(f"[API] Account {account.username} marked as forbidden")
        elif "429" in str(e):
            account.mark_rate_limited()
            logger.warning(f"[API] Account {account.username} rate limited")
        else:
            account.errors += 1
        
        raise HTTPException(
            status_code=500,
            detail=f"Scraping failed: {str(e)}"
        )


@app.post("/scrape/auto", response_model=ScrapeResponse)
async def scrape_subreddits_auto(request: ScrapeRequest):
    """
    Scrape subreddits using all healthy accounts in parallel.
    Subreddits are partitioned across accounts; results are aggregated.
    """
    if not account_pool:
        raise HTTPException(status_code=503, detail="Account pool not initialized")

    if not request.subreddits:
        raise HTTPException(status_code=400, detail="No subreddits provided")

    healthy_accounts = account_pool.get_healthy_accounts()
    if not healthy_accounts:
        raise HTTPException(status_code=503, detail="No healthy accounts available")

    # Partition subreddits across healthy accounts
    sub_chunks = _chunk_list(request.subreddits, len(healthy_accounts))

    start_time = datetime.utcnow()
    try:
        tasks = []
        used_accounts = []
        for acc, subs in zip(healthy_accounts, sub_chunks):
            if not subs:
                continue
            used_accounts.append(acc)
            scraper = RealtimeScraper(
                account=acc,
                max_posts=request.max_posts,
                include_comments=request.include_comments,
                max_comments_per_post=request.max_comments_per_post,
                days_back=request.days_back
            )
            tasks.append(run_scrape_with_fallback(scraper, subs))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Aggregate results and stats
        all_entities = []
        agg_posts = 0
        agg_comments = 0
        agg_errors = 0

        for res in results:
            if isinstance(res, dict):
                all_entities.extend(res.get("data_entities", []))
                st = res.get("stats", {})
                agg_posts += st.get("posts_scraped", 0)
                agg_comments += st.get("comments_scraped", 0)
                agg_errors += st.get("errors", 0)

        duration = (datetime.utcnow() - start_time).total_seconds()

        # Update account stats
        for acc in used_accounts:
            acc.requests_made += 1
            acc.last_used = datetime.utcnow()

        stats = ScrapeStats(
            posts_scraped=agg_posts,
            comments_scraped=agg_comments,
            subreddits_processed=len(request.subreddits),
            duration_seconds=duration,
            account_used="multiple",
            errors_count=agg_errors
        )

        return ScrapeResponse(
            status="success",
            data=all_entities,
            stats=stats,
            message=f"Auto-scraped {stats.posts_scraped} posts from {len(request.subreddits)} subreddit(s) using {len(used_accounts)} account(s)"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Auto scraping failed: {str(e)}")

@app.post("/search", response_model=ScrapeResponse)
async def search_keywords(request: SearchRequest):
    """
    Search subreddits for keywords using all healthy accounts in parallel.
    Each account processes a partition of the subreddit list.
    """
    if not account_pool:
        raise HTTPException(status_code=503, detail="Account pool not initialized")

    if not request.subreddits:
        raise HTTPException(status_code=400, detail="No subreddits provided")
    if not request.keywords:
        raise HTTPException(status_code=400, detail="No keywords provided")

    healthy_accounts = account_pool.get_healthy_accounts()
    if not healthy_accounts:
        raise HTTPException(status_code=503, detail="No healthy accounts available")

    sub_chunks = _chunk_list(request.subreddits, len(healthy_accounts))

    start_time = datetime.utcnow()
    try:
        tasks = []
        used_accounts = []
        for acc, subs in zip(healthy_accounts, sub_chunks):
            if not subs:
                continue
            used_accounts.append(acc)
            scraper = RealtimeScraper(
                account=acc,
                max_posts=request.max_posts,
                include_comments=request.include_comments,
                max_comments_per_post=request.max_comments_per_post,
                days_back=request.days_back
            )
            tasks.append(scraper.search_keywords(subs, request.keywords))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_entities = []
        agg_posts = 0
        agg_comments = 0
        agg_errors = 0

        for res in results:
            if isinstance(res, dict):
                all_entities.extend(res.get("data_entities", []))
                st = res.get("stats", {})
                agg_posts += st.get("posts_scraped", 0)
                agg_comments += st.get("comments_scraped", 0)
                agg_errors += st.get("errors", 0)

        duration = (datetime.utcnow() - start_time).total_seconds()

        # Update accounts used
        for acc in used_accounts:
            acc.requests_made += 1
            acc.last_used = datetime.utcnow()

        stats = ScrapeStats(
            posts_scraped=agg_posts,
            comments_scraped=agg_comments,
            subreddits_processed=len(request.subreddits),
            duration_seconds=duration,
            account_used="multiple",
            errors_count=agg_errors
        )

        return ScrapeResponse(
            status="success",
            data=all_entities,
            stats=stats,
            message=f"Searched {len(request.subreddits)} subreddit(s) for {len(request.keywords)} keyword(s) using {len(used_accounts)} account(s)"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")

@app.post("/scrape/stream")
async def scrape_subreddits_stream(request: ScrapeRequest):
    """
    Scrape subreddits with streaming response
    
    Returns data as it's scraped (server-sent events)
    """
    if not account_pool:
        raise HTTPException(status_code=503, detail="Account pool not initialized")
    
    account = account_pool.get_next_account()
    if not account:
        raise HTTPException(status_code=503, detail="No healthy accounts available")
    
    async def generate_stream():
        """Generate streaming response"""
        try:
            scraper = RealtimeScraper(
                account=account,
                max_posts=request.max_posts,
                include_comments=request.include_comments,
                max_comments_per_post=request.max_comments_per_post,
                days_back=request.days_back
            )
            
            # Stream results as they arrive (with fallback if streaming method missing)
            import json
            if hasattr(scraper, "scrape_subreddits_streaming"):
                async for entity in scraper.scrape_subreddits_streaming(request.subreddits):
                    yield f"data: {json.dumps(entity)}\n\n"
            else:
                # Fallback: sequentially scrape and emit entities
                try:
                    await scraper.initialize()
                    for subreddit in request.subreddits:
                        entities = await scraper.scrape_subreddit(subreddit)
                        for entity_obj in entities:
                            data = entity_obj.to_json_dict()
                            data["subreddit"] = subreddit
                            yield f"data: {json.dumps(data)}\n\n"
                finally:
                    await scraper.cleanup()
        except Exception as e:
            logger.error(f"[STREAM] Error: {e}")
            import json
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
    
    return StreamingResponse(
        generate_stream(),
        media_type="text/event-stream"
    )


# ============================================
# SUPPORT UTILITIES
# ============================================

def _chunk_list(items: List[Any], chunks: int) -> List[List[Any]]:
    """Split a list into N chunks (as evenly as possible)."""
    if chunks <= 0:
        return [items]
    n = max(1, chunks)
    k, m = divmod(len(items), n)
    return [items[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    logger.error(f"[API ERROR] {type(exc).__name__}: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "status": "error",
            "message": str(exc),
            "type": type(exc).__name__
        }
    )

# ============================================
# MAIN ENTRY POINT
# ============================================

if __name__ == "__main__":
    # Run server
    uvicorn.run(
        "reddit_scraper_api:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info",
        access_log=True
    )
