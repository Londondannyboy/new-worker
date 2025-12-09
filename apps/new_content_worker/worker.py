"""
New Content Worker

Clean rewrite of content workflows.
Listens on new-content-queue.
"""

import os
import asyncio
from pathlib import Path
from dotenv import load_dotenv
from temporalio.client import Client, TLSConfig
from temporalio.worker import Worker

# Load environment
load_dotenv()

# Add packages to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Import activities directly from packages
from packages.ai.src.gateway import AIGateway, get_completion_async
from packages.integrations.src.research import crawl4ai_crawl, serper_search
from packages.integrations.src.storage import save_to_neon, sync_to_zep, get_from_neon

# Import workflows
from workflows import CreateCompanyWorkflow, CreateArticleWorkflow, CreateVideoWorkflow, CreateNewsWorkflow
from activities import (
    # Company activities
    normalize_company_url,
    check_company_exists,
    crawl4ai_deep_crawl,
    serper_company_search,
    curate_research_sources,
    check_research_ambiguity,
    generate_company_profile,
    extract_and_process_logo,
    generate_logo_hero_image,
    upload_logo_to_mux,
    save_company_to_neon,
    sync_company_to_zep,
    # Article activities
    research_keywords,
    crawl_topic_urls,
    search_news,
    curate_article_sources,
    get_zep_context,
    generate_four_act_article,
    save_article_to_neon,
    sync_article_to_zep,
    get_article_by_id,
    # News activities
    get_recent_articles,
    assess_news_relevance,
    # Video activities
    generate_video_prompt,
    generate_seedance_video,
    upload_to_mux,
    build_video_narrative,
    update_article_with_video,
)


TASK_QUEUE = os.getenv("TEMPORAL_TASK_QUEUE", "new-content-queue")


async def get_temporal_client() -> Client:
    """Get Temporal client connected to Temporal Cloud."""
    address = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
    namespace = os.getenv("TEMPORAL_NAMESPACE", "default")
    api_key = os.getenv("TEMPORAL_API_KEY")

    if api_key:
        return await Client.connect(
            target_host=address,
            namespace=namespace,
            tls=TLSConfig(),
            api_key=api_key,
        )
    else:
        return await Client.connect(
            target_host=address,
            namespace=namespace,
        )


async def main():
    """Run the content worker."""
    print("=" * 50)
    print("New Content Worker")
    print("=" * 50)
    print(f"Queue: {TASK_QUEUE}")
    print("Workflows: CreateCompanyWorkflow, CreateArticleWorkflow, CreateVideoWorkflow, CreateNewsWorkflow")
    print("=" * 50)

    client = await get_temporal_client()
    print(f"Connected to Temporal: {client.namespace}")

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[
            CreateCompanyWorkflow,
            CreateArticleWorkflow,
            CreateVideoWorkflow,
            CreateNewsWorkflow,
        ],
        activities=[
            # Company
            normalize_company_url,
            check_company_exists,
            crawl4ai_deep_crawl,
            serper_company_search,
            curate_research_sources,
            check_research_ambiguity,
            generate_company_profile,
            extract_and_process_logo,
            generate_logo_hero_image,
            upload_logo_to_mux,
            save_company_to_neon,
            sync_company_to_zep,
            # Article
            research_keywords,
            crawl_topic_urls,
            search_news,
            curate_article_sources,
            get_zep_context,
            generate_four_act_article,
            save_article_to_neon,
            sync_article_to_zep,
            get_article_by_id,
            # News
            get_recent_articles,
            assess_news_relevance,
            # Video
            generate_video_prompt,
            generate_seedance_video,
            upload_to_mux,
            build_video_narrative,
            update_article_with_video,
        ],
    )

    print("Worker running...")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
