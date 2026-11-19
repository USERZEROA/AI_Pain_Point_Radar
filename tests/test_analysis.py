"""Synthetic, explicitly labeled regression fixtures; never used as research data."""
from datetime import datetime, timedelta, timezone
import unittest

from radar.classifier import classify, detect_product
from radar.sequence import MIN_HISTORY, build_signals, evaluate_signals, forecast_series


class ClassifierTests(unittest.TestCase):
    def test_six_categories(self):
        examples = {
            "Cost & limits": "I keep hitting the usage limit in ChatGPT and it is too expensive.",
            "Reliability": "My AI assistant keeps generating wrong answers and I have to verify everything.",
            "Context & memory": "Claude forgets my previous instructions. I have to re-explain the same context.",
            "Workflow & integration": "I keep copying and pasting AI answers between tools because it cannot connect to our documents.",
            "Speed & availability": "I have to wait several minutes for every response from ChatGPT. It is painfully slow.",
            "Privacy & control": "I cannot upload our confidential data to a cloud AI tool.",
        }
        for expected, text in examples.items():
            with self.subTest(expected=expected):
                result = classify(text)
                self.assertTrue(result.is_pain, result)
                self.assertEqual(result.category, expected)
                self.assertLessEqual(len(result.evidence.split()), 25)

    def test_neutral_comment_does_not_inherit_parent_complaint(self):
        result = classify("Thank you for sharing the link.", "ChatGPT is too expensive and unreliable")
        self.assertFalse(result.is_pain)
        self.assertEqual(result.category, "No pain expressed")
        self.assertEqual(result.product, "ChatGPT")

    def test_negated_pain_is_not_a_positive(self):
        for text in ["I don't find ChatGPT expensive.", "It is not slow for me.", "I have no privacy concerns.", "It never hallucinates for me."]:
            with self.subTest(text=text):
                self.assertFalse(classify(text).is_pain)

    def test_contrast_preserves_actual_complaint(self):
        result = classify("I don't find it expensive, but it keeps producing wrong answers.", "Using ChatGPT")
        self.assertEqual(result.category, "Reliability")

    def test_negation_scope_does_not_erase_different_problem(self):
        result = classify("ChatGPT is not slow and I keep getting wrong answers.")
        self.assertEqual(result.category, "Reliability")

    def test_resolved_problem_is_not_an_unresolved_report(self):
        self.assertFalse(classify("I solved the hallucinations by adding a retriever.").is_pain)

    def test_advertising_claim_is_uncertain(self):
        result = classify("We are building an AI app. Our users keep hitting the usage limit. Try our product!")
        self.assertFalse(result.is_pain)
        self.assertEqual(result.category, "Uncertain")

    def test_unattributed_generalization_is_uncertain(self):
        self.assertEqual(classify("All LLMs hallucinate.").category, "Uncertain")

    def test_price_mention_is_not_pain(self):
        self.assertFalse(classify("I use a twenty dollar subscription and it works well.").is_pain)

    def test_ambiguous_product_is_explicit(self):
        self.assertEqual(detect_product("I compared Cursor and ChatGPT"), "Multiple products")
        self.assertEqual(detect_product("Claude Code"), "Claude Code")

    def test_human_memory_and_repetition_are_not_model_memory_failures(self):
        # Regression inspired by public item IDs 50529823, 50524963, 50534136.
        for text in ["I can't remember where I read that.",
                     "I've written code for years, repeating the same patterns. LLMs make it interesting.",
                     "I am trying not to repeat myself while discussing social circles."]:
            with self.subTest(text=text):
                self.assertFalse(classify(text, "Claude Code discussion").is_pain)

    def test_non_digital_connectivity_is_not_integration_pain(self):
        # Regression inspired by public item 50530825, rewritten fixture.
        result = classify("The poem doesn't connect to people. I asked ChatGPT to rewrite it.", "Literature discussion")
        self.assertFalse(result.is_pain)

    def test_none_security_issues_is_negated(self):
        # Regression inspired by public item 50527780, rewritten fixture.
        self.assertFalse(classify("I used Claude Code to fix mail settings. None of them were security issues.").is_pain)

    def test_tutorial_and_launch_copy_paste_are_not_friction(self):
        # Regression inspired by public item 50521067, rewritten fixtures.
        self.assertFalse(classify("My team just released an AI tool. Copy/paste one command to start.").is_pain)
        self.assertFalse(classify("I use ChatGPT. Copy/paste this command to install the helper.").is_pain)

    def test_non_tool_service_cost_does_not_inherit_ai_title(self):
        # Regression inspired by public item 50521472, rewritten fixture.
        self.assertFalse(classify("I worked with software agencies. They are too expensive for small businesses.", "AI and hiring").is_pain)

    def test_requested_timeout_is_not_observed_latency_failure(self):
        # Regression inspired by public item 50519993, rewritten fixture.
        self.assertFalse(classify("I asked the model to add a timeout to an LLM gateway.").is_pain)
        self.assertTrue(classify("I keep getting repeated timeouts from the AI assistant.").is_pain)

    def test_generated_test_rework_requires_specific_combination(self):
        # Regression inspired by public item 50534595; not a verbatim source copy.
        result = classify("Claude keeps creating redundant unit tests. I spent hours removing them by hand.")
        self.assertTrue(result.is_pain)
        self.assertEqual(result.category, "Reliability")
        self.assertFalse(classify("I wrote bad unit tests in my hobby project.").is_pain)
        self.assertFalse(classify("I spent hours writing useful unit tests with Claude.").is_pain)
        self.assertFalse(classify("Claude produced tests that are not bad unit tests. I spent hours reviewing them.").is_pain)

    def test_quoted_claim_is_not_the_reply_authors_experience(self):
        self.assertFalse(classify("> I tested an LLM and got wrong answers.\nI would need to see your prompts to understand this.").is_pain)

    def test_hypothetical_cost_and_instructions_remain_uncertain(self):
        self.assertFalse(classify("Why would I waste tokens on AI when I can use a library?").is_pain)
        self.assertFalse(classify("I use AI tools. You don't need to waste tokens rereading files.").is_pain)

    def test_descriptive_duration_and_broken_down_are_not_failure(self):
        self.assertFalse(classify("I used the AI assistant to build a page. It took thirty minutes.").is_pain)
        self.assertFalse(classify("I want to see AI benefits broken down by department.").is_pain)

    def test_local_tool_pronoun_can_refer_to_previous_user_experience(self):
        result = classify("I use ChatGPT every day. It keeps giving wrong answers.")
        self.assertTrue(result.is_pain)
        self.assertEqual(result.category, "Reliability")

    def test_high_thinking_subscription_is_not_high_cost(self):
        self.assertFalse(classify("I am happy with my ChatGPT subscription. In High thinking mode I get good answers.").is_pain)

    def test_attributed_transcript_is_not_own_feedback(self):
        self.assertFalse(classify("I watched an AI company's video. Transcript: I use an AI assistant and it keeps giving wrong answers.").is_pain)

    def test_external_expenses_do_not_inherit_assistant_context(self):
        self.assertFalse(classify("I want an AI assistant. On my list, I find cheap flights because flying is expensive.", "Gemini assistant").is_pain)

    def test_reproduction_question_is_not_personal_tool_failure(self):
        self.assertFalse(classify("The article is LLM generated. Has anyone reproduced it or is it hallucinated?").is_pain)

    def test_outside_services_do_not_inherit_ai_cyberattack_speculation(self):
        self.assertFalse(classify("I don't believe this is AI hacking. The companies behind outages I've seen say its a focused attack.", "Network outage").is_pain)

    def test_unsolved_external_task_does_not_prove_ai_output_failure(self):
        # Regression inspired by 50559429: rewritten short fixture, not raw text.
        result = classify("I spent an hour debugging my CAD sketch with ChatGPT. I can't get it to work.", "LLM-assisted design")
        self.assertFalse(result.is_pain)
        self.assertEqual(result.category, "Uncertain")
        self.assertIn("attribution uncertain", result.method)
        direct = classify("I used ChatGPT to debug my SQL query. ChatGPT generated invalid SQL and I keep getting wrong answers.")
        self.assertTrue(direct.is_pain)
        self.assertEqual(direct.category, "Reliability")

    def test_explicit_editor_problem_takes_priority_over_token_cost_effect(self):
        # Regression inspired by 50529052: the mechanism is chat editing friction.
        result = classify("My biggest issue is the primitive editor in ChatGPT. I have issues editing markdown text. I press Enter and send an unfinished message and waste tokens.")
        self.assertTrue(result.is_pain)
        self.assertEqual(result.category, "Workflow & integration")
        self.assertFalse(classify("I use ChatGPT. Press Enter to send your message, or add newlines with a keyboard shortcut.").is_pain)

    def test_historical_difficulty_does_not_claim_current_unmet_need(self):
        # Regression inspired by 50555088, deliberately not a verbatim copy.
        result = classify("I ran out of tokens using Claude last week. I have now switched to another model and finally made good progress.")
        self.assertTrue(result.is_pain)
        self.assertEqual(result.category, "Cost & limits")
        self.assertIn("historical difficulty", result.method)
        self.assertIn("alternative/improvement", result.method)


class SequenceTests(unittest.TestCase):
    def setUp(self):
        self.start = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def points(self, counts):
        return [(self.start + timedelta(hours=i), count) for i, count in enumerate(counts)]

    def test_forecasts_do_not_peek_at_current_or_future_counts(self):
        original = forecast_series(self.points([1] * 10 + [3]), "Reliability")
        changed = forecast_series(self.points([1] * 10 + [900, 800, 700]), "Reliability")
        self.assertEqual(original[:10], changed[:10])
        self.assertEqual(original[10].predicted_count, changed[10].predicted_count)
        self.assertNotEqual(original[10].actual_count, changed[10].actual_count)

    def test_warmup_and_clear_spike(self):
        signals = forecast_series(self.points([1] * MIN_HISTORY + [20]), "Reliability")
        self.assertFalse(any(s.is_anomaly for s in signals[:MIN_HISTORY]))
        self.assertTrue(signals[-1].is_anomaly)
        self.assertTrue(signals[-1].forecast_ready)

    def test_single_record_after_zeros_is_not_anomaly(self):
        signals = forecast_series(self.points([0] * 12 + [1]), "Reliability")
        self.assertFalse(signals[-1].is_anomaly)

    def test_boundary_hours_excluded_and_internal_gaps_zero_filled(self):
        rows = [{"window_start": self.start, "category": "Reliability", "record_count": 100},
                {"window_start": self.start + timedelta(hours=2), "category": "Reliability", "record_count": 3},
                {"window_start": self.start + timedelta(hours=4), "category": "Reliability", "record_count": 200}]
        signals = build_signals(rows, self.start + timedelta(minutes=20), self.start + timedelta(hours=4, minutes=5), ["Reliability"])
        self.assertEqual([s.actual_count for s in signals], [0, 3, 0])
        self.assertEqual(signals[0].window_start, self.start + timedelta(hours=1))

    def test_evaluation_uses_same_warmed_up_windows(self):
        signals = forecast_series(self.points([1] * 10 + [4, 2]), "Reliability")
        evaluation = evaluate_signals(signals)
        self.assertEqual(evaluation["evaluated_category_hours"], 4)
        self.assertIsInstance(evaluation["model_mae"], float)
        self.assertIsInstance(evaluation["previous_hour_baseline_mae"], float)

    def test_invalid_counts_or_duplicate_hours_rejected(self):
        with self.assertRaises(ValueError):
            forecast_series(self.points([1, -1]), "Reliability")
        with self.assertRaises(ValueError):
            forecast_series([(self.start, 1), (self.start, 2)], "Reliability")


if __name__ == "__main__":
    unittest.main()
