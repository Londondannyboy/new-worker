"""
Job Worker

Temporal worker for job scraping workflows.
Listens on quest-job-queue.
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

# Import packages
from packages.ai.src.gateway import AIGateway

# Import workflows
from workflows import (
    JobScrapingWorkflow,
    GreenhouseScraperWorkflow,
    LeverScraperWorkflow,
    AshbyScraperWorkflow,
    GenericScraperWorkflow,
)

# Import activities
from activities import (
    get_companies_to_scrape,
    scrape_greenhouse_jobs,
    scrape_lever_jobs,
    scrape_ashby_jobs,
    scrape_generic_jobs,
    extract_job_skills,
    save_jobs_to_database,
    sync_jobs_to_zep,
    calculate_company_trends,
)


TASK_QUEUE = os.getenv("JOB_WORKER_TASK_QUEUE", "quest-job-queue")


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
    """Run the job worker."""
    print("=" * 50)
    print("Job Worker")
    print("=" * 50)
    print(f"Queue: {TASK_QUEUE}")
    print("Workflows: JobScrapingWorkflow, GreenhouseScraperWorkflow, LeverScraperWorkflow, AshbyScraperWorkflow, GenericScraperWorkflow")
    print("=" * 50)

    client = await get_temporal_client()
    print(f"Connected to Temporal: {client.namespace}")

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[
            JobScrapingWorkflow,
            GreenhouseScraperWorkflow,
            LeverScraperWorkflow,
            AshbyScraperWorkflow,
            GenericScraperWorkflow,
        ],
        activities=[
            get_companies_to_scrape,
            scrape_greenhouse_jobs,
            scrape_lever_jobs,
            scrape_ashby_jobs,
            scrape_generic_jobs,
            extract_job_skills,
            save_jobs_to_database,
            sync_jobs_to_zep,
            calculate_company_trends,
        ],
    )

    print("Worker running...")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
