"""
Company Worker

Temporal worker for company-related workflows.
Connects to Temporal Cloud and processes CreateCompanyWorkflow.
"""

import os
import asyncio
from dotenv import load_dotenv
from temporalio.client import Client, TLSConfig
from temporalio.worker import Worker

# Load environment
load_dotenv()

# Import workflows and activities
from src.workflows import CreateCompanyWorkflow
from src.activities import (
    normalize_company_url,
    check_company_exists,
    crawl4ai_deep_crawl,
    serper_company_search,
    curate_research_sources,
    check_research_ambiguity,
    generate_company_profile,
    extract_and_process_logo,
    save_company_to_neon,
    sync_company_to_zep,
)


TASK_QUEUE = "quest-company-queue"


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
    """Run the company worker."""
    print(f"Starting Company Worker on queue: {TASK_QUEUE}")

    client = await get_temporal_client()

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[CreateCompanyWorkflow],
        activities=[
            normalize_company_url,
            check_company_exists,
            crawl4ai_deep_crawl,
            serper_company_search,
            curate_research_sources,
            check_research_ambiguity,
            generate_company_profile,
            extract_and_process_logo,
            save_company_to_neon,
            sync_company_to_zep,
        ],
    )

    print(f"Worker connected to Temporal")
    print(f"Registered workflows: CreateCompanyWorkflow")
    print(f"Registered activities: {len(worker._activities)}")

    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
