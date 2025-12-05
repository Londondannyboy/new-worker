"""
Job Worker Workflows

Job scraping and enrichment workflows.
"""

from datetime import timedelta
from typing import Dict, Any, List
from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
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


@workflow.defn
class JobScrapingWorkflow:
    """Master workflow that orchestrates all job scraping.

    Input:
        companies: Optional list of specific companies to scrape
                  If None, fetches all active companies from database

    Output:
        total_companies: int
        total_jobs_found: int
        total_jobs_added: int
        results: List of per-company results
    """

    @workflow.run
    async def run(self, input_dict: Dict[str, Any]) -> Dict[str, Any]:
        companies = input_dict.get("companies")

        workflow.logger.info("JobScrapingWorkflow starting")

        # Get companies to scrape
        if companies is None:
            result = await workflow.execute_activity(
                get_companies_to_scrape,
                args=["jobs"],
                start_to_close_timeout=timedelta(seconds=30),
            )
            companies = result.get("companies", [])

        if not companies:
            return {
                "success": True,
                "total_companies": 0,
                "total_jobs_found": 0,
                "total_jobs_added": 0,
                "results": [],
                "message": "No companies to scrape"
            }

        workflow.logger.info(f"Scraping {len(companies)} companies")

        # Group companies by board type
        companies_by_type: Dict[str, List] = {}
        for company in companies:
            board_type = company.get("board_type", "unknown")
            if board_type not in companies_by_type:
                companies_by_type[board_type] = []
            companies_by_type[board_type].append(company)

        # Map board types to workflows
        workflow_map = {
            "greenhouse": "GreenhouseScraperWorkflow",
            "lever": "LeverScraperWorkflow",
            "ashby": "AshbyScraperWorkflow",
            "unknown": "GenericScraperWorkflow",
        }

        # Launch child workflows
        child_handles = []
        for board_type, type_companies in companies_by_type.items():
            workflow_name = workflow_map.get(board_type, "GenericScraperWorkflow")

            for company in type_companies:
                company_slug = company["name"].lower().replace(" ", "-")[:30]
                handle = await workflow.start_child_workflow(
                    workflow_name,
                    {"company": company},
                    id=f"scrape-{company_slug}-{workflow.uuid4().hex[:6]}",
                    retry_policy=RetryPolicy(maximum_attempts=2),
                )
                child_handles.append((company["name"], handle))

        # Collect results
        results = []
        for company_name, handle in child_handles:
            try:
                result = await handle
                results.append(result)
            except Exception as e:
                results.append({
                    "company_name": company_name,
                    "jobs_found": 0,
                    "jobs_added": 0,
                    "errors": [str(e)],
                })

        # Calculate trends for companies with new jobs
        companies_with_jobs = [r["company_name"] for r in results if r.get("jobs_added", 0) > 0]
        if companies_with_jobs:
            await workflow.execute_activity(
                calculate_company_trends,
                args=[companies_with_jobs],
                start_to_close_timeout=timedelta(minutes=2),
            )

        total_found = sum(r.get("jobs_found", 0) for r in results)
        total_added = sum(r.get("jobs_added", 0) for r in results)

        workflow.logger.info(f"JobScrapingWorkflow complete: {total_found} found, {total_added} added")

        return {
            "success": True,
            "total_companies": len(companies),
            "total_jobs_found": total_found,
            "total_jobs_added": total_added,
            "results": results,
        }


@workflow.defn
class GreenhouseScraperWorkflow:
    """Scrape jobs from Greenhouse boards."""

    @workflow.run
    async def run(self, input_dict: Dict[str, Any]) -> Dict[str, Any]:
        company = input_dict.get("company", {})
        start_time = workflow.now()

        workflow.logger.info(f"Scraping Greenhouse: {company.get('name')}")

        # Scrape jobs
        scrape_result = await workflow.execute_activity(
            scrape_greenhouse_jobs,
            args=[company],
            start_to_close_timeout=timedelta(minutes=3),
            retry_policy=RetryPolicy(maximum_attempts=2),
        )

        jobs = scrape_result.get("jobs", [])
        if not jobs:
            return {
                "company_name": company.get("name"),
                "jobs_found": 0,
                "jobs_added": 0,
                "jobs_updated": 0,
                "errors": [scrape_result.get("error")] if scrape_result.get("error") else [],
            }

        # Extract skills
        enrich_result = await workflow.execute_activity(
            extract_job_skills,
            args=[jobs],
            start_to_close_timeout=timedelta(minutes=3),
        )
        enriched_jobs = enrich_result.get("jobs", jobs)

        # Save to database
        db_result = await workflow.execute_activity(
            save_jobs_to_database,
            args=[{"company": company, "jobs": enriched_jobs}],
            start_to_close_timeout=timedelta(minutes=2),
        )

        # Sync new jobs to Zep
        if db_result.get("added", 0) > 0:
            await workflow.execute_activity(
                sync_jobs_to_zep,
                args=[enriched_jobs, company.get("name")],
                start_to_close_timeout=timedelta(minutes=2),
            )

        duration = (workflow.now() - start_time).total_seconds()

        return {
            "company_name": company.get("name"),
            "jobs_found": len(jobs),
            "jobs_added": db_result.get("added", 0),
            "jobs_updated": db_result.get("updated", 0),
            "errors": db_result.get("errors", []),
            "duration_seconds": duration,
        }


@workflow.defn
class LeverScraperWorkflow:
    """Scrape jobs from Lever boards."""

    @workflow.run
    async def run(self, input_dict: Dict[str, Any]) -> Dict[str, Any]:
        company = input_dict.get("company", {})
        start_time = workflow.now()

        workflow.logger.info(f"Scraping Lever: {company.get('name')}")

        # Scrape jobs
        scrape_result = await workflow.execute_activity(
            scrape_lever_jobs,
            args=[company],
            start_to_close_timeout=timedelta(minutes=3),
            retry_policy=RetryPolicy(maximum_attempts=2),
        )

        jobs = scrape_result.get("jobs", [])
        if not jobs:
            return {
                "company_name": company.get("name"),
                "jobs_found": 0,
                "jobs_added": 0,
                "jobs_updated": 0,
                "errors": [scrape_result.get("error")] if scrape_result.get("error") else [],
            }

        # Extract skills
        enrich_result = await workflow.execute_activity(
            extract_job_skills,
            args=[jobs],
            start_to_close_timeout=timedelta(minutes=3),
        )
        enriched_jobs = enrich_result.get("jobs", jobs)

        # Save to database
        db_result = await workflow.execute_activity(
            save_jobs_to_database,
            args=[{"company": company, "jobs": enriched_jobs}],
            start_to_close_timeout=timedelta(minutes=2),
        )

        # Sync to Zep
        if db_result.get("added", 0) > 0:
            await workflow.execute_activity(
                sync_jobs_to_zep,
                args=[enriched_jobs, company.get("name")],
                start_to_close_timeout=timedelta(minutes=2),
            )

        duration = (workflow.now() - start_time).total_seconds()

        return {
            "company_name": company.get("name"),
            "jobs_found": len(jobs),
            "jobs_added": db_result.get("added", 0),
            "jobs_updated": db_result.get("updated", 0),
            "errors": db_result.get("errors", []),
            "duration_seconds": duration,
        }


@workflow.defn
class AshbyScraperWorkflow:
    """Scrape jobs from Ashby boards using Crawl4AI."""

    @workflow.run
    async def run(self, input_dict: Dict[str, Any]) -> Dict[str, Any]:
        company = input_dict.get("company", {})
        start_time = workflow.now()

        workflow.logger.info(f"Scraping Ashby: {company.get('name')}")

        # Scrape jobs
        scrape_result = await workflow.execute_activity(
            scrape_ashby_jobs,
            args=[company],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=2),
        )

        jobs = scrape_result.get("jobs", [])
        if not jobs:
            return {
                "company_name": company.get("name"),
                "jobs_found": 0,
                "jobs_added": 0,
                "jobs_updated": 0,
                "errors": [scrape_result.get("error")] if scrape_result.get("error") else [],
            }

        # Extract skills (if descriptions available)
        enrich_result = await workflow.execute_activity(
            extract_job_skills,
            args=[jobs],
            start_to_close_timeout=timedelta(minutes=3),
        )
        enriched_jobs = enrich_result.get("jobs", jobs)

        # Save to database
        db_result = await workflow.execute_activity(
            save_jobs_to_database,
            args=[{"company": company, "jobs": enriched_jobs}],
            start_to_close_timeout=timedelta(minutes=2),
        )

        # Sync to Zep
        if db_result.get("added", 0) > 0:
            await workflow.execute_activity(
                sync_jobs_to_zep,
                args=[enriched_jobs, company.get("name")],
                start_to_close_timeout=timedelta(minutes=2),
            )

        duration = (workflow.now() - start_time).total_seconds()

        return {
            "company_name": company.get("name"),
            "jobs_found": len(jobs),
            "jobs_added": db_result.get("added", 0),
            "jobs_updated": db_result.get("updated", 0),
            "errors": db_result.get("errors", []),
            "duration_seconds": duration,
        }


@workflow.defn
class GenericScraperWorkflow:
    """Fallback scraper using AI extraction."""

    @workflow.run
    async def run(self, input_dict: Dict[str, Any]) -> Dict[str, Any]:
        company = input_dict.get("company", {})
        start_time = workflow.now()

        workflow.logger.info(f"Scraping generic: {company.get('name')}")

        # Scrape jobs with AI
        scrape_result = await workflow.execute_activity(
            scrape_generic_jobs,
            args=[company],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=2),
        )

        jobs = scrape_result.get("jobs", [])
        if not jobs:
            return {
                "company_name": company.get("name"),
                "jobs_found": 0,
                "jobs_added": 0,
                "jobs_updated": 0,
                "errors": [scrape_result.get("error")] if scrape_result.get("error") else [],
            }

        # Extract skills
        enrich_result = await workflow.execute_activity(
            extract_job_skills,
            args=[jobs],
            start_to_close_timeout=timedelta(minutes=3),
        )
        enriched_jobs = enrich_result.get("jobs", jobs)

        # Save to database
        db_result = await workflow.execute_activity(
            save_jobs_to_database,
            args=[{"company": company, "jobs": enriched_jobs}],
            start_to_close_timeout=timedelta(minutes=2),
        )

        # Sync to Zep
        if db_result.get("added", 0) > 0:
            await workflow.execute_activity(
                sync_jobs_to_zep,
                args=[enriched_jobs, company.get("name")],
                start_to_close_timeout=timedelta(minutes=2),
            )

        duration = (workflow.now() - start_time).total_seconds()

        return {
            "company_name": company.get("name"),
            "jobs_found": len(jobs),
            "jobs_added": db_result.get("added", 0),
            "jobs_updated": db_result.get("updated", 0),
            "errors": db_result.get("errors", []),
            "duration_seconds": duration,
        }
