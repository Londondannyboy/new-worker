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
                        args=[video_result.get("video_url"), article_id, app, article.get("title")],
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


@workflow.defn
class CreateVideoWorkflow:
    """Generate a video for an existing article.

    Takes an article_id and generates a 4-act video from the article's
    four_act_content, then uploads to MUX and updates the article.

    Input:
        article_id: ID of the article to generate video for
        video_quality: "low", "medium", or "high" (default: "medium")
        character_style: Optional style hint for video

    Output:
        success: bool
        article_id: str
        playback_id: str (MUX playback ID)
        cost: float
    """

    @workflow.run
    async def run(self, input_dict: Dict[str, Any]) -> Dict[str, Any]:
        article_id = input_dict.get("article_id")
        video_quality = input_dict.get("video_quality", "medium")
        character_style = input_dict.get("character_style")

        if not article_id:
            return {"success": False, "error": "article_id is required"}

        workflow.logger.info(f"CreateVideoWorkflow starting for article: {article_id}")
        start_time = workflow.now()

        # Phase 1: Get article
        article_result = await workflow.execute_activity(
            get_article_by_id,
            args=[article_id],
            start_to_close_timeout=timedelta(seconds=30),
        )
        if not article_result.get("success"):
            return {"success": False, "error": article_result.get("error")}

        article = article_result.get("article", {})
        four_act_content = article.get("four_act_content", [])
        app = article.get("app", "placement")
        title = article.get("title", "")

        # Check if already has video
        if article.get("playback_id"):
            workflow.logger.info(f"Article already has video: {article.get('playback_id')}")
            return {
                "success": True,
                "article_id": article_id,
                "playback_id": article.get("playback_id"),
                "status": "already_exists",
                "cost": 0,
            }

        # Phase 2: Generate video prompt
        prompt_result = await workflow.execute_activity(
            generate_video_prompt,
            args=[four_act_content, app, character_style],
            start_to_close_timeout=timedelta(seconds=30),
        )
        if not prompt_result.get("success"):
            return {"success": False, "error": f"Failed to generate prompt: {prompt_result.get('error')}"}

        # Phase 3: Generate video with Seedance
        video_result = await workflow.execute_activity(
            generate_seedance_video,
            args=[prompt_result.get("prompt"), video_quality],
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=RetryPolicy(maximum_attempts=2),
        )
        if not video_result.get("success"):
            return {"success": False, "error": f"Video generation failed: {video_result.get('error')}"}

        video_url = video_result.get("video_url")
        total_cost = video_result.get("cost", 0)

        # Phase 4: Upload to MUX (with title for dashboard)
        mux_result = await workflow.execute_activity(
            upload_to_mux,
            args=[video_url, article_id, app, title],
            start_to_close_timeout=timedelta(minutes=5),
        )
        if not mux_result.get("success"):
            return {"success": False, "error": f"MUX upload failed: {mux_result.get('error')}", "cost": total_cost}

        playback_id = mux_result.get("playback_id")
        asset_id = mux_result.get("asset_id")

        # Phase 5: Build video narrative
        video_narrative = await workflow.execute_activity(
            build_video_narrative,
            args=[playback_id, four_act_content],
            start_to_close_timeout=timedelta(seconds=30),
        )

        # Phase 6: Update article with video
        await workflow.execute_activity(
            update_article_with_video,
            args=[article_id, playback_id, asset_id, video_narrative],
            start_to_close_timeout=timedelta(seconds=30),
        )

        duration = (workflow.now() - start_time).total_seconds()
        workflow.logger.info(f"Video created for article {article_id}: {playback_id}")

        return {
            "success": True,
            "article_id": article_id,
            "playback_id": playback_id,
            "asset_id": asset_id,
            "cost": total_cost,
            "duration_seconds": duration,
        }


@workflow.defn
class CreateNewsWorkflow:
    """Monitor news and create articles for relevant stories.

    Scheduled workflow that:
    1. Searches for news via Serper
    2. Gets recent articles to avoid duplicates
    3. AI assesses relevance and priority
    4. Spawns CreateArticleWorkflow for top stories

    Input:
        app: "placement" or "relocation"
        keywords: List of keywords to search
        max_articles: Max articles to create (default: 3)
        min_relevance: Minimum relevance score (default: 0.6)
        generate_video: Whether to generate videos (default: True)

    Output:
        success: bool
        stories_found: int
        stories_relevant: int
        articles_created: List of created articles
    """

    @workflow.run
    async def run(self, input_dict: Dict[str, Any]) -> Dict[str, Any]:
        app = input_dict.get("app", "placement")
        keywords = input_dict.get("keywords", [])
        max_articles = input_dict.get("max_articles", 3)
        min_relevance = input_dict.get("min_relevance", 0.6)
        generate_video = input_dict.get("generate_video", True)
        video_quality = input_dict.get("video_quality", "medium")

        # Default keywords by app
        if not keywords:
            keywords = {
                "placement": ["private equity news", "M&A deals", "corporate finance"],
                "relocation": ["digital nomad visa", "expat news", "international relocation"]
            }.get(app, ["news"])

        total_cost = 0.0
        workflow.logger.info(f"CreateNewsWorkflow starting for: {app}")
        workflow.logger.info(f"Keywords: {keywords}")

        # Phase 1: Search news
        all_stories = []
        for keyword in keywords[:3]:  # Limit to 3 keywords
            news_result = await workflow.execute_activity(
                search_news,
                args=[keyword, 10],
                start_to_close_timeout=timedelta(minutes=2),
            )
            all_stories.extend(news_result.get("articles", []))
            total_cost += news_result.get("cost", 0)

        # Deduplicate by URL
        seen_urls = set()
        unique_stories = []
        for story in all_stories:
            url = story.get("url", "").lower().split("?")[0]
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique_stories.append(story)

        workflow.logger.info(f"Found {len(unique_stories)} unique stories")

        if not unique_stories:
            return {
                "success": True,
                "app": app,
                "stories_found": 0,
                "stories_relevant": 0,
                "articles_created": [],
                "message": "No news stories found"
            }

        # Phase 2: Get recent articles for duplicate check
        recent_result = await workflow.execute_activity(
            get_recent_articles,
            args=[app, 7, 50],
            start_to_close_timeout=timedelta(seconds=30),
        )
        recent_titles = [a.get("title", "") for a in recent_result.get("articles", [])]

        # Phase 3: AI assessment
        assessment_result = await workflow.execute_activity(
            assess_news_relevance,
            args=[unique_stories, app, recent_titles],
            start_to_close_timeout=timedelta(minutes=5),
        )
        total_cost += assessment_result.get("cost", 0)

        relevant_stories = assessment_result.get("relevant_stories", [])
        workflow.logger.info(f"Assessment: {len(relevant_stories)} relevant stories")

        # Phase 4: Create articles for top stories
        articles_created = []

        for story_data in relevant_stories[:max_articles]:
            story = story_data.get("story", {})
            priority = story_data.get("priority", "medium")
            title = story.get("title", "")

            workflow.logger.info(f"Creating article: {title[:50]}...")

            # Build article input
            article_input = {
                "topic": title,
                "app": app,
                "article_type": "news",
                "word_count": 1500,
                "generate_video": generate_video and priority in ["high", "medium"],
                "video_quality": video_quality,
            }

            try:
                # Execute child workflow
                result = await workflow.execute_child_workflow(
                    "CreateArticleWorkflow",
                    article_input,
                    id=f"news-article-{workflow.uuid4().hex[:8]}",
                    task_queue=workflow.info().task_queue,
                )

                if result.get("success"):
                    articles_created.append({
                        "article_id": result.get("article_id"),
                        "slug": result.get("slug"),
                        "title": result.get("title"),
                        "priority": priority,
                        "video_playback_id": result.get("video_playback_id"),
                    })
                    total_cost += result.get("total_cost", 0)
                    workflow.logger.info(f"Article created: {result.get('slug')}")
                else:
                    workflow.logger.warning(f"Article creation failed: {result.get('error')}")

            except Exception as e:
                workflow.logger.error(f"Failed to create article: {e}")

        workflow.logger.info(f"CreateNewsWorkflow complete: {len(articles_created)} articles created")

        return {
            "success": True,
            "app": app,
            "stories_found": len(unique_stories),
            "stories_relevant": len(relevant_stories),
            "articles_created": articles_created,
            "high_priority": assessment_result.get("total_high", 0),
            "medium_priority": assessment_result.get("total_medium", 0),
            "low_priority": assessment_result.get("total_low", 0),
            "total_cost": total_cost,
        }
