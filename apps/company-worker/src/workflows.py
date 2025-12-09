"""
CreateCompanyWorkflow

Clean implementation following the plan:
1. NORMALIZE - Normalize URL, check if exists
2. RESEARCH - Crawl4AI + Serper (parallel)
3. CURATE - AI ranks sources
4. CHECK AMBIGUITY - Stop if unclear
5. GENERATE PROFILE - AI via Pydantic Gateway
6. MEDIA - Logo, optional video
7. SAVE - Neon + Zep
8. COMPLETE - Return result
"""

from datetime import timedelta
from typing import Dict, Any
from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    # Import activities - try relative first, then absolute for content-worker
    try:
        from .activities import (
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
        )
    except ImportError:
        from company_activities import (
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
        )


@workflow.defn
class CreateCompanyWorkflow:
    """
    Create a company profile from a URL.

    Uses Pydantic AI Gateway for all AI calls.
    """

    @workflow.run
    async def run(self, input_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute company creation workflow.

        Args:
            input_dict: {
                "url": "https://example.com",
                "category": "private equity",
                "app": "placement",
                "jurisdiction": "UK",
                "force_update": False,
                "generate_video": False
            }

        Returns:
            Dict with company_id, slug, success status
        """
        url = input_dict.get("url", "")
        category = input_dict.get("category", "")
        app = input_dict.get("app", "placement")
        jurisdiction = input_dict.get("jurisdiction", "UK")
        force_update = input_dict.get("force_update", False)
        generate_video = input_dict.get("generate_video", False)

        total_cost = 0.0

        workflow.logger.info(f"CreateCompanyWorkflow starting for: {url}")

        # ===== PHASE 1: NORMALIZE =====
        workflow.logger.info("Phase 1: Normalize URL")

        normalize_result = await workflow.execute_activity(
            normalize_company_url,
            args=[url, category],
            start_to_close_timeout=timedelta(seconds=30),
        )

        normalized_url = normalize_result.get("url", url)
        domain = normalize_result.get("domain", "")

        workflow.logger.info(f"Normalized: {domain}")

        # Check if company exists
        if not force_update:
            exists = await workflow.execute_activity(
                check_company_exists,
                args=[domain],
                start_to_close_timeout=timedelta(seconds=10),
            )
            if exists:
                workflow.logger.info(f"Company already exists: {domain}")
                return {
                    "success": True,
                    "status": "exists",
                    "domain": domain,
                    "message": "Company already exists. Use force_update=True to regenerate.",
                }

        # ===== PHASE 2: RESEARCH (parallel) =====
        workflow.logger.info("Phase 2: Research (parallel)")

        # Execute research activities in parallel
        crawl_result, serper_result = await workflow.execute_activity(
            crawl4ai_deep_crawl,
            args=[normalized_url, 10],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=2),
        ), await workflow.execute_activity(
            serper_company_search,
            args=[domain],
            start_to_close_timeout=timedelta(minutes=2),
        )

        crawled_pages = crawl_result.get("pages", [])
        news_articles = serper_result.get("results", [])
        total_cost += serper_result.get("cost", 0)

        workflow.logger.info(
            f"Research complete: {len(crawled_pages)} pages, {len(news_articles)} news"
        )

        if not crawled_pages and not news_articles:
            return {
                "success": False,
                "error": "No research data found",
                "domain": domain,
            }

        # ===== PHASE 3: CURATE =====
        workflow.logger.info("Phase 3: Curate research sources")

        curate_result = await workflow.execute_activity(
            curate_research_sources,
            args=[crawled_pages, news_articles, category],
            start_to_close_timeout=timedelta(minutes=2),
        )

        ranked_sources = curate_result.get("sources", [])
        confidence = curate_result.get("confidence", 0)
        total_cost += curate_result.get("cost", 0)

        workflow.logger.info(f"Curation complete: {len(ranked_sources)} sources, confidence={confidence}")

        # ===== PHASE 4: CHECK AMBIGUITY =====
        workflow.logger.info("Phase 4: Check ambiguity")

        ambiguity_result = await workflow.execute_activity(
            check_research_ambiguity,
            args=[ranked_sources, domain],
            start_to_close_timeout=timedelta(seconds=30),
        )

        if ambiguity_result.get("is_ambiguous", False):
            workflow.logger.warning("Research is ambiguous - needs human review")
            return {
                "success": False,
                "needs_human_review": True,
                "ambiguity_signals": ambiguity_result.get("signals", []),
                "domain": domain,
            }

        # ===== PHASE 5: GENERATE PROFILE (AI) =====
        workflow.logger.info("Phase 5: Generate profile via Pydantic AI Gateway")

        profile_result = await workflow.execute_activity(
            generate_company_profile,
            args=[ranked_sources, domain, category, app, jurisdiction],
            start_to_close_timeout=timedelta(minutes=3),
        )

        if not profile_result.get("success"):
            return {
                "success": False,
                "error": profile_result.get("error", "Profile generation failed"),
                "domain": domain,
            }

        profile = profile_result.get("profile", {})
        total_cost += profile_result.get("cost", 0)

        workflow.logger.info(f"Profile generated: {profile.get('legal_name', domain)}")

        # ===== PHASE 6: MEDIA =====
        workflow.logger.info("Phase 6: Process media (logo + hero image)")

        logo_result = await workflow.execute_activity(
            extract_and_process_logo,
            args=[normalized_url, profile.get("legal_name", domain)],
            start_to_close_timeout=timedelta(minutes=2),
        )

        logo_url = logo_result.get("logo_url")
        hero_playback_id = None
        thumbnail_playback_id = None

        if logo_url:
            profile["logo_url"] = logo_url

            # Generate hero image from logo
            workflow.logger.info("Phase 6.5: Generate hero image")
            hero_result = await workflow.execute_activity(
                generate_logo_hero_image,
                args=[
                    logo_url,
                    profile.get("legal_name", domain),
                    jurisdiction,
                    category,
                ],
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=RetryPolicy(maximum_attempts=2),
            )

            if hero_result.get("success") and hero_result.get("image_url"):
                hero_image_url = hero_result.get("image_url")

                # Upload to MUX (we'll get company_id after save, so temporarily store URL)
                profile["hero_image_url"] = hero_image_url

        # TODO: Video generation if generate_video=True
        video_playback_id = None

        # ===== PHASE 7: SAVE =====
        workflow.logger.info("Phase 7: Save to Neon + Zep")

        # Save to Neon
        save_result = await workflow.execute_activity(
            save_company_to_neon,
            args=[profile, app],
            start_to_close_timeout=timedelta(seconds=30),
        )

        company_id = save_result.get("id")

        if not company_id:
            return {
                "success": False,
                "error": "Failed to save to database",
                "domain": domain,
            }

        # Upload hero image to MUX if generated
        if profile.get("hero_image_url"):
            workflow.logger.info("Phase 7.5: Upload hero image to MUX")
            mux_result = await workflow.execute_activity(
                upload_logo_to_mux,
                args=[
                    profile.get("hero_image_url"),
                    company_id,
                    profile.get("legal_name", domain),
                ],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=RetryPolicy(maximum_attempts=2),
            )

            if mux_result.get("success"):
                hero_playback_id = mux_result.get("hero_playback_id")
                thumbnail_playback_id = mux_result.get("thumbnail_playback_id")
                profile["hero_playback_id"] = hero_playback_id
                profile["thumbnail_playback_id"] = thumbnail_playback_id

        # Sync to Zep
        await workflow.execute_activity(
            sync_company_to_zep,
            args=[company_id, profile, app],
            start_to_close_timeout=timedelta(seconds=30),
        )

        # ===== PHASE 8: COMPLETE =====
        workflow.logger.info(f"CreateCompanyWorkflow complete: {profile.get('slug')}")

        return {
            "success": True,
            "company_id": company_id,
            "slug": profile.get("slug"),
            "domain": domain,
            "legal_name": profile.get("legal_name"),
            "logo_url": profile.get("logo_url"),
            "hero_playback_id": hero_playback_id,
            "thumbnail_playback_id": thumbnail_playback_id,
            "video_playback_id": video_playback_id,
            "cost": total_cost,
        }
