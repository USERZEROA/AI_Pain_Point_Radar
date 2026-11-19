"""Conservative, inspectable baseline for first-person AI tool pain reports.

This is not a sentiment model and its labels are not validated market demand.
The full parent title may identify a product, but never supplies complaint evidence
for an otherwise neutral comment. A single primary category is returned per item.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import re
from typing import Any, Mapping

RULE_VERSION = "pain-rules-1.2.0"
PAIN_CATEGORIES = (
    "Cost & limits", "Reliability", "Context & memory",
    "Workflow & integration", "Speed & availability", "Privacy & control",
)
ALL_CATEGORIES = (*PAIN_CATEGORIES, "No pain expressed", "Uncertain")

# Patterns deliberately describe a problem, rather than merely naming a topic.
PATTERNS: dict[str, tuple[str, ...]] = {
    "Cost & limits": (
        r"\b(?:too|so|very|prohibitively|ridiculously) expensive\b",
        r"\b(?:is|feels|becomes|getting|gets|find)\b.{0,15}\bexpensive\b",
        r"\b(?:can(?:not|'t|’t)|could(?:not|n't)|unable to) afford\b",
        r"\b(?:hit|hitting|reached|exceed(?:ed|ing)?|runs? into)\b.{0,35}\b(?:rate|usage|message|token|daily|weekly)?\s*limits?\b",
        r"\b(?:run|ran|running) out of (?:credits|tokens|quota|messages)\b",
        r"\brate[- ]limit(?:ed|ing)?\b",
        r"\b(?:costs?|pricing|bill|subscription)\b.{0,35}\b(?:expensive|unpredictable|confusing|doubled|increased|too much)\b",
        r"\b(?:costs?|pricing|bill)\b.{0,20}\bhigh\b(?!\s+thinking)",
        r"\b(?:burn(?:ing|s)?|wast(?:e|es|ing|ed))\b.{0,30}\b(?:tokens|credits|money|budget)\b",
        r"\bpaywall(?:ed)?\b",
    ),
    "Reliability": (
        r"\bhallucinat(?:e|es|ed|ing|ion|ions)\b",
        r"\b(?:incorrect|inaccurate|unreliable|made[- ]up|fabricated)\b",
        r"\b(?:keeps?|kept|often|frequently|still)\b.{0,30}\b(?:wrong|break(?:s|ing)?|fail(?:s|ing|ed)?|bugs?)\b",
        r"\b(?:gave|gives|getting|got|returns?|produces?|generates?)\b.{0,30}\b(?:wrong|broken|invalid|duplicate)\b",
        r"\b(?:broke|broken(?!\s+down\b)|regressions?|duplicate code|dead code)\b",
        r"\b(?:does(?:n't| not)|can(?:'t|not))\b.{0,25}\b(?:work(?!\s+(?:fast|quickly|slowly))|compile|run correctly|follow instructions)\b",
        r"\b(?:manually|have to|need to)\b.{0,25}\b(?:verify|fact[- ]check|double[- ]check|fix everything)\b",
    ),
    "Context & memory": (
        r"\b(?:forgets?|forgetting|forgot)\b.{0,45}\b(?:context|instructions?|conversation|earlier|previous|told|requirements?)\b",
        r"\b(?:loses?|lost|losing)\b.{0,25}\b(?:context|memory|track)\b",
        r"\bcontext (?:window|limit)\b.{0,30}\b(?:small|short|limited|limit|hit|full)\b",
        r"\b(?:hit|exceed(?:ed)?|run out of)\b.{0,25}\bcontext\b",
        r"\b(?:repeat|repeating|re[- ]explain)\b.{0,35}\b(?:context|instructions?|requirements?|prompts?)\b",
        r"\brepeat(?:ing)? myself\b.{0,30}\b(?:assistant|model|ChatGPT|Claude|AI|LLM)\b",
        r"\b(?:it|the (?:model|assistant)|ChatGPT|Claude|AI|LLM)\b.{0,15}\b(?:does(?:n't| not)|can(?:'t|not)) remember\b",
        r"\b(?:memory|context)\b.{0,25}\b(?:unreliable|missing|lost|truncated|forgotten)\b",
    ),
    "Workflow & integration": (
        r"\b(?:copy(?:ing)?[- /]?(?:and[- /]?)?past(?:e|ing)|copy and paste)\b",
        r"\b(?:integration|integrating|workflow)\b.{0,35}\b(?:hard|difficult|painful|clunky|missing|broken|friction)\b",
        r"\b(?:can(?:'t|not)|does(?:n't| not)|unable to)\b.{0,20}\b(?:integrate|connect|export|import)\b.{0,35}\b(?:API|tools?|files?|data|documents?|apps?|databases?|email|calendar|IDE|systems?|workflow|repositories|repos?)\b",
        r"\b(?:switch(?:ing)?|jump(?:ing)?) between\b.{0,35}\b(?:tools|apps|tabs|services)\b",
        r"\bmanually\b.{0,30}\b(?:transfer|paste|export|import|move data|sync)\b",
        r"\b(?:lack|missing|no)\b.{0,20}\b(?:integration|connector|export|api access)\b",
        r"\b(?:workflow|process)\b.{0,25}\b(?:interrupt(?:ed|ions?)?|fragmented)\b",
    ),
    "Speed & availability": (
        r"\b(?:too|so|very|painfully|unbearably) slow\b",
        r"\b(?:latency|response time)\b.{0,30}\b(?:high|bad|slow|unacceptable|increased)\b",
        r"\b(?:times? out|timed out|unresponsive|unavailable|outages?|downtime)\b",
        r"\b(?:getting|hitting|frequent|repeated|constant|suffer(?:ing)?|frustrated by)\b.{0,20}\btimeouts?\b",
        r"\bwait(?:ing|ed)?\b.{0,20}\b(?:minutes|forever|ages)\b",
        r"\b(?:keeps?|often|constantly)\b.{0,20}\b(?:crash(?:ing|es)?|disconnect(?:ing|s)?|hang(?:ing|s)?)\b",
        r"\b(?:isn't|is not|not) fast enough\b",
    ),
    "Privacy & control": (
        r"\b(?:privacy|security)\b.{0,25}\b(?:concerns?|risks?|issues?|worries|problem)\b",
        r"\b(?:concerned|worried|uncomfortable)\b.{0,40}\b(?:privacy|data|confidential|upload|sensitive)\b",
        r"\b(?:can(?:'t|not)|won't|will not|not allowed to) upload\b.{0,35}\b(?:data|documents?|code|files?|confidential|sensitive)\b",
        r"\b(?:data leak(?:age)?|leaking (?:data|secrets)|confidential data|sensitive data)\b",
        r"\b(?:don't|do not|can't|cannot) trust\b.{0,30}\b(?:data|privacy|cloud|upload)\b",
        r"\b(?:need|require|must have)\b.{0,35}\b(?:local|on[- ]prem|offline)\b.{0,30}\b(?:privacy|data|security)\b",
    ),
}
COMPILED = {name: tuple(re.compile(p, re.I) for p in patterns) for name, patterns in PATTERNS.items()}
USER_SIGNAL = re.compile(
    r"\b(?:I|I'm|I've|I'd|I'll|my|me|we|we're|we've|our|us)\b|"
    r"\b(?:anyone else|does anyone|anybody else|has anyone)\b|"
    r"\b(?:struggling|frustrated|experiencing|in my experience)\b", re.I
)
PROMOTIONAL = re.compile(
    r"\b(?:we(?:'re| are)|I(?:'m| am)) (?:building|launching|selling)\b|"
    r"\b(?:try (?:our|my)|sign up|check out (?:our|my)|our product|we built|I built)\b|"
    r"\b(?:my team|our team|we|I)\b.{0,18}\b(?:released|launched|introducing)\b|"
    r"\b(?:we'd|we would) love (?:for )?you to try\b", re.I
)
NEGATED_PROBLEM = re.compile(
    r"\b(?:not|never|no longer|isn't|aren't|wasn't)\s+(?:particularly\s+|really\s+|that\s+|too\s+)?"
    r"(?:expensive|slow|unreliable|incorrect|inaccurate|broken|a problem)\b|"
    r"\b(?:never|doesn't|does not)\s+hallucinat\w*\b|"
    r"\b(?:don't|do not|never)\s+(?:find|think|feel)\b.{0,35}\b(?:expensive|slow|unreliable)\b|"
    r"\b(?:fixed|solved|eliminated|resolved)\b.{0,30}\b(?:hallucinations?|timeouts?|outages?|privacy concerns?)\b|"
    r"\b(?:none|neither|not)\b.{0,25}\b(?:security|privacy) issues?\b|"
    r"\b(?:no|without)\s+(?:privacy concerns?|security risks?|timeouts?|outages?|hallucinations?)\b",
    re.I,
)
GENERAL_FRICTION = re.compile(r"\b(?:frustrat\w*|struggl\w*|annoy\w*|painful|disappoint\w*|hate|problem)\b", re.I)
AI_OBJECT = re.compile(
    r"\b(?:AI|LLMs?|GPT\w*|ChatGPT|Claude|Codex|Copilot|Gemini|DeepSeek|Ollama|OpenAI|"
    r"chatbots?|assistants?|language models?|coding agents?|models?|prompts?|tokens?|context window)\b", re.I
)
DIRECT_EXPERIENCE = re.compile(
    r"\b(?:I|we)(?:'ve|'m|'re| have| had| am| are)?\s+(?:use|used|using|tried|try|keep|kept|"
    r"have to|had to|need|needed|can(?:'t|not)|could(?:n't| not)|wait|waited|find|found|"
    r"spent|spend|took|take|experience|experienced|ran|run|hit|hitting|get|got|lost|lose|"
    r"asked|want|wanted|struggle|struggling|work|working|pay|paid|waste|wasted|seen|notice|noticed|observed|press|pressed|edit|editing)\b|"
    r"\b(?:I|we) (?:have|had|often have|keep having) (?:issues|problems|trouble) (?:using|editing|integrating|accessing)\b|"
    r"\b(?:my|our)\s+(?:AI|assistant|model|ChatGPT|Claude|workflow|data|subscription|code|system|session|messages?|requests?|tokens?|credits?)\b|"
    r"\b(?:my|our)(?:\s+\w+){0,2}\s+(?:instructions?|requirements?|prompts?)\b|"
    r"\b(?:I|we) (?:don't|do not) (?:find|experience|see)\b|"
    r"\b(?:anyone else|does anyone|anybody else|has anyone)\b.{0,50}\b(?:experience|experienced|getting|hit|hitting|trouble|problems?|issues?)\b|"
    r"\b(?:in my experience|I'm frustrated|I am frustrated)\b", re.I
)
WORKFLOW_FRICTION = re.compile(
    r"\b(?:have to|had to|need to|keep|kept|repeatedly|constantly|manually|tedious|painful|"
    r"frustrat\w*|annoy\w*|hard|difficult|missing|lack|can't|cannot|doesn't|does not|unable|no)\b", re.I
)
LOW_QUALITY_TEST = re.compile(
    r"\b(?:bad|useless|redundant|pointless|meaningless|dumb|bullshit|low[- ]quality)\b.{0,20}\b(?:unit tests?|test cases?|tests?)\b", re.I
)
MANUAL_REWORK = re.compile(
    r"\b(?:I|we)\b.{0,75}\b(?:hours?|days?)\b|\b(?:hand remove|manually (?:remove|fix|rewrite|delete)|by myself|by hand)\b", re.I
)
HYPOTHETICAL = re.compile(r"\b(?:why would|would have|could have|if we|if I|if you|don't need to|do not need to)\b", re.I)
AI_COST_OBJECT = re.compile(r"\b(?:AI|LLMs?|ChatGPT|Claude|Codex|Copilot|Gemini|OpenAI|tokens?|credits?|quota|usage|rate[- ]limits?|subscription|plan|API|model)\b", re.I)
EDITOR_FRICTION = re.compile(
    r"\b(?:issues?|trouble|problems?) editing\b.{0,45}\b(?:messages?|markdown|strings?|text)\b|"
    r"\b(?:no way|can't|cannot|unable)\b.{0,45}\b(?:insert newlines?|line breaks?|edit messages?)\b|"
    r"\b(?:press|hit)\b.{0,20}\b(?:Enter|Return)\b.{0,45}\b(?:unfinished|accidentally|unintentionally)\b", re.I
)
PRIMARY_EDITOR_PROBLEM = re.compile(
    r"\b(?:biggest issue|main problem|main issue|most frustrating)\b.{0,65}\b(?:editor|editing|chat interface|newlines?|keyboard)\b", re.I
)
AI_ASSISTED_DEBUGGING = re.compile(
    r"\b(?:debugging|troubleshooting|diagnosing)\b.{0,30}\b(?:with|using)\b.{0,15}\b(?:ChatGPT|Claude|AI|LLM|assistant)\b|"
    r"\b(?:used|using|asked)\b.{0,15}\b(?:ChatGPT|Claude|AI|LLM|assistant)\b.{0,30}\b(?:debug|troubleshoot|diagnose)\b", re.I
)
EXPLICIT_AI_OUTPUT_FAILURE = re.compile(
    r"\b(?:ChatGPT|Claude|AI|LLM|model|assistant)\b.{0,25}\b(?:gave|gives|generated|generates|returned|returns|produced|produces|invented|invents)\b.{0,35}\b(?:wrong|incorrect|invalid|broken|fabricated|nonexistent)\b|"
    r"\b(?:ChatGPT|Claude|AI|LLM|model|assistant)\b.{0,25}\bhallucinat\w*\b", re.I
)
PAST_DIFFICULTY = re.compile(r"\b(?:ran out|used to|previously|last (?:week|month|year)|for about a year)\b", re.I)
LATER_IMPROVEMENT = re.compile(
    r"\bnow\b.{0,25}\b(?:use|using|tried|switched)\b|\bswitched to\b|"
    r"\bfinally\b.{0,25}\b(?:progress|working|works|solved)\b|\b(?:resolved|solved) (?:by|with)\b", re.I
)


def _experience(text: str) -> bool:
    # Comprehension/opinion phrases such as 'I get that' are not usage reports.
    without_opinion = re.sub(r"\bI (?:get that|want to see|think|believe|suspect|understand)\b", "", text, flags=re.I)
    return bool(DIRECT_EXPERIENCE.search(without_opinion))


def _tool_context(clause: str, previous_clause: str, title: str) -> bool:
    """Require a local AI object or a narrowly resolved tool pronoun.

    A title may resolve 'it' or 'the tool', but merely being in an AI discussion
    does not turn a complaint about people, agencies, or poetry into AI feedback.
    """
    if AI_OBJECT.search(clause):
        return True
    tool_pronoun = re.match(r"(?:it|its|this tool|the tool|the service)\b", clause, re.I)
    personal_tool_ref = re.search(r"\b(?:I|we) (?:can't|cannot|keep|have to)\b.{0,30}\bit\b", clause, re.I)
    technical_object = re.search(r"\b(?:responses?|generated code|usage limits?)\b", clause, re.I)
    if tool_pronoun or personal_tool_ref or technical_object:
        return bool(AI_OBJECT.search(previous_clause) or AI_OBJECT.search(title))
    return False

PRODUCT_PATTERNS: tuple[tuple[str, str], ...] = (
    ("Claude Code", r"\bclaude\s+code\b"),
    ("ChatGPT", r"\bchat\s?gpt\b"),
    ("Codex", r"\bcodex\b"),
    ("Cursor", r"\bcursor\b"),
    ("GitHub Copilot", r"\b(?:github\s+)?copilot\b"),
    ("Gemini", r"\bgemini\b"),
    ("DeepSeek", r"\bdeepseek\b"),
    ("Perplexity", r"\bperplexity\b"),
    ("Ollama", r"\bollama\b"),
    ("Claude", r"\bclaude\b"),
    ("OpenAI API", r"\bopenai\s+(?:api|models?)\b"),
)


@dataclass(frozen=True)
class Classification:
    product: str
    category: str
    is_pain: bool
    evidence: str
    method: str = "conservative first-person rules"
    rule_version: str = RULE_VERSION

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def detect_product(text: str, title: str = "") -> str:
    """Keep ambiguous multiple-product comparisons explicit instead of guessing."""
    combined = f"{text}\n{title}"
    found = [name for name, pattern in PRODUCT_PATTERNS if re.search(pattern, combined, re.I)]
    if "Claude Code" in found and "Claude" in found:
        found.remove("Claude")
    if len(found) > 1:
        return "Multiple products"
    return found[0] if found else "Unspecified"


def _excerpt(text: str, limit: int = 24) -> str:
    words = text.strip().split()
    clipped = " ".join(words[:limit])
    return clipped + (" …" if len(words) > limit else "")


def classify(text: str, title: str = "", item_type: str = "comment") -> Classification:
    """Label only text containing actual problem evidence; title is context only.

    First-person or question cues improve specificity. General assertions and
    promotional claims remain Uncertain. A literal negative term is insufficient
    when its local clause explicitly negates the problem.
    """
    product = detect_product(text, title)
    # Quoted replies are someone else's statement, not this author's experience.
    # Transcript tails are attributed dialogue, not the comment author's report.
    own_text = re.split(r"\btranscript\s*:", text, maxsplit=1, flags=re.I)[0]
    unquoted = re.sub(r"(?m)^\s*>[^\n]*", "", own_text)
    normalized = re.sub(r"\s+", " ", unquoted.replace("’", "'")).strip()
    if not normalized:
        return Classification(product, "Uncertain", False, "Empty text; no classification evidence.")
    clauses = [s.strip() for s in re.split(r"(?<=[.!?])[\"'”’)]*\s+|[;\n]+|\bbut\b|\bhowever\b", normalized, flags=re.I) if s.strip()]
    # A personal account may establish the speaker in an earlier sentence.
    personal = bool(USER_SIGNAL.search(normalized) and _experience(normalized))
    promotional = bool(PROMOTIONAL.search(normalized))
    scores: dict[str, int] = {category: 0 for category in PAIN_CATEGORIES}
    evidence: dict[str, str] = {}
    weak_problem = False
    previous_clause = ""
    for clause in clauses:
        negations = [match.span() for match in NEGATED_PROBLEM.finditer(clause)]
        for category, patterns in COMPILED.items():
            matches = sum(any(
                not any(match.start() < end and match.end() > start for start, end in negations)
                for match in pattern.finditer(clause)
            ) for pattern in patterns)
            if matches:
                local_experience = _experience(clause)
                referred_experience = bool(
                    re.search(r"\b(?:it|its|this tool|the tool|the assistant)\b", clause, re.I)
                    and _experience(previous_clause)
                    and (AI_OBJECT.search(previous_clause) or AI_OBJECT.search(title))
                )
                if HYPOTHETICAL.search(clause) or not (local_experience or referred_experience):
                    weak_problem = True
                    continue
                if not _tool_context(clause, previous_clause, title):
                    weak_problem = True
                    continue
                if category == "Workflow & integration" and not WORKFLOW_FRICTION.search(clause):
                    weak_problem = True
                    continue
                if category == "Cost & limits" and not AI_COST_OBJECT.search(clause):
                    if not (re.match(r"(?:it|this tool|the tool|this service)\b", clause, re.I) and referred_experience):
                        weak_problem = True
                        continue
                scores[category] += matches
                evidence.setdefault(category, clause)
        previous_clause = clause
    # A specific missed user workflow found during review: low-quality generated
    # tests requiring manual rework. Neither profanity nor 'bad tests' alone is
    # sufficient. Preserve a primary-label baseline rather than add an ad hoc ID.
    low_quality = LOW_QUALITY_TEST.search(normalized)
    if low_quality and MANUAL_REWORK.search(normalized) and AI_OBJECT.search(normalized):
        prefix = normalized[max(0, low_quality.start() - 12):low_quality.start()]
        if not re.search(r"\b(?:not|aren't|isn't|weren't)\s*$", prefix, re.I):
            scores["Reliability"] += 2
            evidence.setdefault("Reliability", next((clause for clause in clauses if LOW_QUALITY_TEST.search(clause)), normalized))
    editor_matches = list(EDITOR_FRICTION.finditer(normalized))
    if editor_matches and personal and AI_OBJECT.search(normalized):
        # Identify the reported mechanism. In an explicit primary editor complaint,
        # accidental token spending is an impact rather than the primary category.
        scores["Workflow & integration"] += len(editor_matches)
        if PRIMARY_EDITOR_PROBLEM.search(normalized):
            scores["Workflow & integration"] += 2
        evidence.setdefault("Workflow & integration", next((clause for clause in clauses if EDITOR_FRICTION.search(clause)), normalized))
    winner = max(PAIN_CATEGORIES, key=lambda category: scores[category])
    if scores[winner] == 0:
        if weak_problem or (GENERAL_FRICTION.search(normalized) and personal):
            return Classification(product, "Uncertain", False, _excerpt(normalized))
        return Classification(product, "No pain expressed", False, "No explicit supported pain statement matched.")
    if promotional or not personal:
        return Classification(product, "Uncertain", False, _excerpt(evidence[winner]))
    if winner == "Reliability" and AI_ASSISTED_DEBUGGING.search(normalized) and not EXPLICIT_AI_OUTPUT_FAILURE.search(normalized):
        # A user's CAD/database/app problem remaining unsolved with AI assistance
        # does not establish that the AI produced an erroneous output.
        return Classification(product, "Uncertain", False, _excerpt(evidence[winner]),
                              method="conservative first-person rules; external-task failure attribution uncertain")
    method = "conservative first-person rules"
    if PAST_DIFFICULTY.search(normalized) and LATER_IMPROVEMENT.search(normalized):
        method += "; historical difficulty; alternative/improvement mentioned"
    return Classification(product, winner, True, _excerpt(evidence[winner]), method=method)


def classify_event(event: Any) -> Classification:
    """Accept the Event dataclass or a mapping, keeping the classifier easy to audit."""
    if isinstance(event, Mapping):
        return classify(str(event.get("text", "")), str(event.get("title", "")), str(event.get("item_type", "comment")))
    return classify(event.text, event.title, event.item_type)
