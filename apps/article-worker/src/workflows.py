"""
CreateArticleWorkflow

Clean implementation following the plan:
1. KEYWORD RESEARCH - If not provided
2. RESEARCH - Crawl4AI + Serper (parallel)
3. CURATE - AI ranks sources
4. ZEP CONTEXT - Knowledge graph
5. GENERATE ARTICLE - AI 4-act structure
6. SAVE - Neon (draft)
7. SYNC ZEP - Knowledge graph
8. VIDEO PROMPT - Generate from visual hints
9. VIDEO - Seedance + MUX (optional)
10. COMPLETE - Return result
"""

from datetime import timedelta
from typing import Dict, Any
from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    # Import activities - try relative first, then absolute for content-worker
    try:
        from .activities import (
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
    except ImportError:
        from article_activities import (
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
class CreateArticleWorkflow:
    """
    Create an article from a topic.

    Uses Pydantic AI Gateway for all AI calls.
    Generates 4-act structure with optional video.
    """

    @workflow.run
    async def run(self, input_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute article creation workflow.

        Args:
            input_dict: {
                "topic": "Private Equity Trends 2024",
                "app": "placement",
                "article_type": "standard",
                "keywords": [],
                "target_keyword": None,
                "word_count": 2000,
                "jurisdiction": "UK",
                "video_quality": "medium",
                "video_model": "seedance",
                "character_style": None,
                "custom_slug": None,
                "generate_video": True
            }

        Returns:
            Dict with article_id, slug, video_playback_id, success status
        """
        topic = input_dict.get("topic", "")
        app = input_dict.get("app", "placement")
        article_type = input_dict.get("article_type", "standard")
        keywords = input_dict.get("keywords", [])
        target_keyword = input_dict.get("target_keyword")
        word_count = input_dict.get("word_count", 2000)
        jurisdiction = input_dict.get("jurisdiction", "UK")
        video_quality = input_dict.get("video_quality", "medium")
        video_model = input_dict.get("video_model", "seedance")
        character_style = input_dict.get("character_style")
        custom_slug = input_dict.get("custom_slug")
        generate_video = input_dict.get("generate_video", True)

        total_cost = 0.0
        start_time = workflow.now()

        workflow.logger.info(f"CreateArticleWorkflow starting for: {topic}")

        # ===== PHASE 0: KEYWORD RESEARCH =====
        if not target_keyword:
            workflow.logger.info("Phase 0: Keyword research")

            keyword_result = await workflow.execute_activity(
                research_keywords,
                args=[topic, jurisdiction],
                start_to_close_timeout=timedelta(seconds=60),
            )

            target_keyword = keyword_result.get("target_keyword", topic)
            secondary_keywords = keyword_result.get("secondary_keywords", [])
        else:
            secondary_keywords = keywords

        workflow.logger.info(f"Target keyword: {target_keyword}")

        # ===== PHASE 1-2: RESEARCH (parallel) =====
        workflow.logger.info("Phase 1-2: Research (parallel)")

        # Execute research activities
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

        workflow.logger.info(
            f"Research complete: {len(crawled_pages)} pages, {len(news_articles)} news"
        )

        if not crawled_pages and not news_articles:
            return {
                "success": False,
                "error": "No research data found",
                "topic": topic,
            }

        # ===== PHASE 3: CURATE SOURCES =====
        workflow.logger.info("Phase 3: Curate research sources")

        curate_result = await workflow.execute_activity(
            curate_article_sources,
            args=[crawled_pages, news_articles, topic],
            start_to_close_timeout=timedelta(minutes=2),
        )

        ranked_sources = curate_result.get("sources", [])
        key_facts = curate_result.get("key_facts", [])
        outline = curate_result.get("outline", [])
        total_cost += curate_result.get("cost", 0)

        workflow.logger.info(f"Curation complete: {len(ranked_sources)} sources")

        # ===== PHASE 4: ZEP CONTEXT =====
        workflow.logger.info("Phase 4: Query Zep for context")

        zep_context = await workflow.execute_activity(
            get_zep_context,
            args=[topic, app],
            start_to_close_timeout=timedelta(seconds=30),
        )

        # ===== PHASE 5: GENERATE 4-ACT ARTICLE =====
        workflow.logger.info("Phase 5: Generate 4-act article via AI Gateway")

        article_result = await workflow.execute_activity(
            generate_four_act_article,
            args=[
                topic,
                ranked_sources,
                key_facts,
                outline,
                word_count,
                target_keyword,
                secondary_keywords,
                app,
                article_type,
            ],
            start_to_close_timeout=timedelta(minutes=5),
        )

        if not article_result.get("success"):
            return {
                "success": False,
                "error": article_result.get("error", "Article generation failed"),
                "topic": topic,
            }

        article = article_result.get("article", {})
        four_act_content = article_result.get("four_act_content", [])
        total_cost += article_result.get("cost", 0)

        workflow.logger.info(f"Article generated: {article.get('title')}")

        # ===== PHASE 6: SAVE TO DATABASE =====
        workflow.logger.info("Phase 6: Save to Neon (draft)")

        save_result = await workflow.execute_activity(
            save_article_to_neon,
            args=[article, four_act_content, app, ranked_sources],
            start_to_close_timeout=timedelta(seconds=30),
        )

        article_id = save_result.get("id")

        if not article_id:
            return {
                "success": False,
                "error": "Failed to save to database",
                "topic": topic,
            }

        # ===== PHASE 7: SYNC TO ZEP =====
        workflow.logger.info("Phase 7: Sync to Zep knowledge graph")

        await workflow.execute_activity(
            sync_article_to_zep,
            args=[article_id, article, key_facts, app],
            start_to_close_timeout=timedelta(seconds=30),
        )

        # ===== PHASE 8: VIDEO PROMPT =====
        video_playback_id = None

        if generate_video and video_quality != "none":
            workflow.logger.info("Phase 8: Generate video prompt")

            prompt_result = await workflow.execute_activity(
                generate_video_prompt,
                args=[four_act_content, app, character_style],
                start_to_close_timeout=timedelta(seconds=30),
            )

            if prompt_result.get("success"):
                # ===== PHASE 9: GENERATE VIDEO =====
                workflow.logger.info("Phase 9: Generate Seedance video")

                video_result = await workflow.execute_activity(
                    generate_seedance_video,
                    args=[prompt_result.get("prompt"), video_quality],
                    start_to_close_timeout=timedelta(minutes=10),
                )

                if video_result.get("success") and video_result.get("video_url"):
                    # ===== PHASE 10: UPLOAD TO MUX =====
                    workflow.logger.info("Phase 10: Upload to MUX")

                    mux_result = await workflow.execute_activity(
                        upload_to_mux,
                        args=[video_result.get("video_url"), article_id, app],
                        start_to_close_timeout=timedelta(minutes=5),
                    )

                    if mux_result.get("success"):
                        video_playback_id = mux_result.get("playback_id")

                        # ===== PHASE 11: BUILD NARRATIVE =====
                        video_narrative = await workflow.execute_activity(
                            build_video_narrative,
                            args=[video_playback_id, four_act_content],
                            start_to_close_timeout=timedelta(seconds=30),
                        )

                        # ===== PHASE 12: UPDATE ARTICLE =====
                        await workflow.execute_activity(
                            update_article_with_video,
                            args=[
                                article_id,
                                video_playback_id,
                                mux_result.get("asset_id"),
                                video_narrative,
                            ],
                            start_to_close_timeout=timedelta(seconds=30),
                        )

                    total_cost += video_result.get("cost", 0)

        # ===== COMPLETE =====
        duration = (workflow.now() - start_time).total_seconds()

        workflow.logger.info(f"CreateArticleWorkflow complete: {article.get('slug')}")

        return {
            "success": True,
            "article_id": article_id,
            "slug": article.get("slug"),
            "title": article.get("title"),
            "video_playback_id": video_playback_id,
            "word_count": article.get("word_count"),
            "section_count": 4,
            "total_cost": total_cost,
            "duration_seconds": duration,
        }
