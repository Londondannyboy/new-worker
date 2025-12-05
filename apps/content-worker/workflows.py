"""
Content Worker Workflows

CreateCompanyWorkflow and CreateArticleWorkflow combined.
"""

from datetime import timedelta
from typing import Dict, Any
from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
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
        generate_video_prompt,
        generate_seedance_video,
        upload_to_mux,
        build_video_narrative,
        update_article_with_video,
    )


@workflow.defn
class CreateCompanyWorkflow:
    """Create a company profile from a URL."""

    @workflow.run
    async def run(self, input_dict: Dict[str, Any]) -> Dict[str, Any]:
        url = input_dict.get("url", "")
        category = input_dict.get("category", "")
        app = input_dict.get("app", "placement")
        jurisdiction = input_dict.get("jurisdiction", "UK")
        force_update = input_dict.get("force_update", False)

        total_cost = 0.0
        workflow.logger.info(f"CreateCompanyWorkflow starting for: {url}")

        # Phase 1: Normalize
        normalize_result = await workflow.execute_activity(
            normalize_company_url,
            args=[url, category],
            start_to_close_timeout=timedelta(seconds=30),
        )
        normalized_url = normalize_result.get("url", url)
        domain = normalize_result.get("domain", "")

        # Check if exists
        if not force_update:
            exists_result = await workflow.execute_activity(
                check_company_exists,
                args=[domain],
                start_to_close_timeout=timedelta(seconds=10),
            )
            if exists_result.get("exists"):
                return {
                    "success": True,
                    "status": "exists",
                    "domain": domain,
                    "company_id": exists_result.get("company_id"),
                    "slug": exists_result.get("slug"),
                }

        # Phase 2: Research
        crawl_result = await workflow.execute_activity(
            crawl4ai_deep_crawl,
            args=[normalized_url, 10],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=2),
        )
        serper_result = await workflow.execute_activity(
            serper_company_search,
            args=[domain],
            start_to_close_timeout=timedelta(minutes=2),
        )

        crawled_pages = crawl_result.get("pages", [])
        news_articles = serper_result.get("results", [])
        total_cost += serper_result.get("cost", 0)

        if not crawled_pages and not news_articles:
            return {"success": False, "error": "No research data found", "domain": domain}

        # Phase 3: Curate
        curate_result = await workflow.execute_activity(
            curate_research_sources,
            args=[crawled_pages, news_articles, category],
            start_to_close_timeout=timedelta(minutes=2),
        )
        ranked_sources = curate_result.get("sources", [])
        total_cost += curate_result.get("cost", 0)

        # Phase 4: Check ambiguity
        ambiguity_result = await workflow.execute_activity(
            check_research_ambiguity,
            args=[ranked_sources, domain],
            start_to_close_timeout=timedelta(seconds=30),
        )
        if ambiguity_result.get("is_ambiguous", False):
            return {"success": False, "needs_human_review": True, "domain": domain}

        # Phase 5: Generate profile
        profile_result = await workflow.execute_activity(
            generate_company_profile,
            args=[ranked_sources, domain, category, app, jurisdiction],
            start_to_close_timeout=timedelta(minutes=3),
        )
        if not profile_result.get("success"):
            return {"success": False, "error": profile_result.get("error"), "domain": domain}

        profile = profile_result.get("profile", {})
        total_cost += profile_result.get("cost", 0)

        # Phase 6: Media
        logo_result = await workflow.execute_activity(
            extract_and_process_logo,
            args=[normalized_url, profile.get("legal_name", domain)],
            start_to_close_timeout=timedelta(minutes=2),
        )
        if logo_result.get("logo_url"):
            profile["logo_url"] = logo_result.get("logo_url")

        # Phase 7: Save
        save_result = await workflow.execute_activity(
            save_company_to_neon,
            args=[profile, app],
            start_to_close_timeout=timedelta(seconds=30),
        )
        company_id = save_result.get("id")
        if not company_id:
            return {"success": False, "error": "Failed to save", "domain": domain}

        await workflow.execute_activity(
            sync_company_to_zep,
            args=[company_id, profile, app],
            start_to_close_timeout=timedelta(seconds=30),
        )

        return {
            "success": True,
            "company_id": company_id,
            "slug": profile.get("slug"),
            "domain": domain,
            "cost": total_cost,
        }


@workflow.defn
class CreateArticleWorkflow:
    """Create a 4-act article from a topic."""

    @workflow.run
    async def run(self, input_dict: Dict[str, Any]) -> Dict[str, Any]:
        topic = input_dict.get("topic", "")
        app = input_dict.get("app", "placement")
        article_type = input_dict.get("article_type", "standard")
        keywords = input_dict.get("keywords", [])
        target_keyword = input_dict.get("target_keyword")
        word_count = input_dict.get("word_count", 2000)
        jurisdiction = input_dict.get("jurisdiction", "UK")
        video_quality = input_dict.get("video_quality", "medium")
        generate_video = input_dict.get("generate_video", True)
        character_style = input_dict.get("character_style")

        total_cost = 0.0
        start_time = workflow.now()
        workflow.logger.info(f"CreateArticleWorkflow starting for: {topic}")

        # Phase 0: Keyword research
        if not target_keyword:
            keyword_result = await workflow.execute_activity(
                research_keywords,
                args=[topic, jurisdiction],
                start_to_close_timeout=timedelta(seconds=60),
            )
            target_keyword = keyword_result.get("target_keyword", topic)
            secondary_keywords = keyword_result.get("secondary_keywords", [])
        else:
            secondary_keywords = keywords

        # Phase 1-2: Research
        crawl_result = await workflow.execute_activity(
            crawl_topic_urls,
            args=[topic, 10],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=2),
        )
        news_result = await workflow.execute_activity(
            search_news,
            args=[topic, 10],
            start_to_close_timeout=timedelta(minutes=2),
        )

        crawled_pages = crawl_result.get("pages", [])
        news_articles = news_result.get("articles", [])
        total_cost += news_result.get("cost", 0)

        if not crawled_pages and not news_articles:
            return {"success": False, "error": "No research data found", "topic": topic}

        # Phase 3: Curate
        curate_result = await workflow.execute_activity(
            curate_article_sources,
            args=[crawled_pages, news_articles, topic],
            start_to_close_timeout=timedelta(minutes=2),
        )
        ranked_sources = curate_result.get("sources", [])
        key_facts = curate_result.get("key_facts", [])
        outline = curate_result.get("outline", [])
        total_cost += curate_result.get("cost", 0)

        # Phase 4: Zep context
        await workflow.execute_activity(
            get_zep_context,
            args=[topic, app],
            start_to_close_timeout=timedelta(seconds=30),
        )

        # Phase 5: Generate article
        article_result = await workflow.execute_activity(
            generate_four_act_article,
            args=[topic, ranked_sources, key_facts, outline, word_count,
                  target_keyword, secondary_keywords, app, article_type],
            start_to_close_timeout=timedelta(minutes=5),
        )
        if not article_result.get("success"):
            return {"success": False, "error": article_result.get("error"), "topic": topic}

        article = article_result.get("article", {})
        four_act_content = article_result.get("four_act_content", [])
        total_cost += article_result.get("cost", 0)

        # Phase 6: Save
        save_result = await workflow.execute_activity(
            save_article_to_neon,
            args=[article, four_act_content, app, ranked_sources],
            start_to_close_timeout=timedelta(seconds=30),
        )
        article_id = save_result.get("id")
        if not article_id:
            return {"success": False, "error": "Failed to save", "topic": topic}

        # Phase 7: Sync to Zep
        await workflow.execute_activity(
            sync_article_to_zep,
            args=[article_id, article, key_facts, app],
            start_to_close_timeout=timedelta(seconds=30),
        )

        # Phase 8-12: Video (optional)
        video_playback_id = None
        if generate_video and video_quality != "none":
            prompt_result = await workflow.execute_activity(
                generate_video_prompt,
                args=[four_act_content, app, character_style],
                start_to_close_timeout=timedelta(seconds=30),
            )
            if prompt_result.get("success"):
                video_result = await workflow.execute_activity(
                    generate_seedance_video,
                    args=[prompt_result.get("prompt"), video_quality],
                    start_to_close_timeout=timedelta(minutes=10),
                )
                if video_result.get("success") and video_result.get("video_url"):
                    mux_result = await workflow.execute_activity(
                        upload_to_mux,
                        args=[video_result.get("video_url"), article_id, app],
                        start_to_close_timeout=timedelta(minutes=5),
                    )
                    if mux_result.get("success"):
                        video_playback_id = mux_result.get("playback_id")
                        video_narrative = await workflow.execute_activity(
                            build_video_narrative,
                            args=[video_playback_id, four_act_content],
                            start_to_close_timeout=timedelta(seconds=30),
                        )
                        await workflow.execute_activity(
                            update_article_with_video,
                            args=[article_id, video_playback_id,
                                  mux_result.get("asset_id"), video_narrative],
                            start_to_close_timeout=timedelta(seconds=30),
                        )
                    total_cost += video_result.get("cost", 0)

        duration = (workflow.now() - start_time).total_seconds()
        return {
            "success": True,
            "article_id": article_id,
            "slug": article.get("slug"),
            "title": article.get("title"),
            "video_playback_id": video_playback_id,
            "word_count": article.get("word_count"),
            "total_cost": total_cost,
            "duration_seconds": duration,
        }
