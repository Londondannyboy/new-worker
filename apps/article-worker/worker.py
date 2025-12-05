"""
Article Worker

Temporal worker for article-related workflows.
Connects to Temporal Cloud and processes CreateArticleWorkflow.
"""

import os
import asyncio
from dotenv import load_dotenv
from temporalio.client import Client, TLSConfig
from temporalio.worker import Worker

# Load environment
load_dotenv()

# Import workflows and activities
from src.workflows import CreateArticleWorkflow
from src.activities import (
    research_keywords,
    crawl_topic_urls,
    search_news,
    curate_article_sources,
    get_zep_context,
    generate_four_act_article,
    save_article_to_neon,
    sync_article_to_zep,
    generate_video_prompt,
    generate_seedance_video,
    upload_to_mux,
    build_video_narrative,
    update_article_with_video,
)


TASK_QUEUE = "quest-article-queue"


async def get_temporal_client() -> Client:
    """
    Get Temporal client connected to Temporal Cloud.
    """
    address = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
    namespace = os.getenv("TEMPORAL_NAMESPACE", "default")
    api_key = os.getenv("TEMPORAL_API_KEY")

    if api_key:
        # Temporal Cloud connection
        return await Client.connect(
            target_host=address,
            namespace=namespace,
            tls=TLSConfig(),
            api_key=api_key,
        )
    else:
        # Local development
        return await Client.connect(
            target_host=address,
            namespace=namespace,
        )


async def main():
    """Run the article worker."""
    print(f"Starting Article Worker on queue: {TASK_QUEUE}")

    client = await get_temporal_client()

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[CreateArticleWorkflow],
        activities=[
            research_keywords,
            crawl_topic_urls,
            search_news,
            curate_article_sources,
            get_zep_context,
            generate_four_act_article,
            save_article_to_neon,
            sync_article_to_zep,
            generate_video_prompt,
            generate_seedance_video,
            upload_to_mux,
            build_video_narrative,
            update_article_with_video,
        ],
    )

    print(f"Worker connected to Temporal")
    print(f"Registered workflows: CreateArticleWorkflow")
    print(f"Registered activities: {len(worker._activities)}")

    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
