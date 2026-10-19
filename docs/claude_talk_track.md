# Claude demo talk track - natural-language questions (tested 2026-10-18/19)

Setup: Claude Desktop, a NEW chat inside a Claude Project whose only instructions are docs/mcp_demo_prompt.md
(data dictionary, no answers). Both MCP servers connected (quantic-couchbase, quantic-analytics). Not the Quantic
working project - its docs contain the planted stories. Answers vary run to run; "Expect" is a sanity check, not a script.

## 0. Start first: the discovery question (~5 min - ask it, then talk architecture while it runs)
Ask: "Which merchants are at risk of churning?"
- Expect: Claude studies the merchants that already left, finds the pattern (flat, then a fast drop 2-4 months out),
  and notices most churn is whole chains leaving at once. Watch list of a few softening merchants, with caveats.
- Say: "It learned what churn looks like from your own history before answering - and told us how much to trust it."

## 1. Live operations (Query service, last 90 days - seconds)
Ask: "Who at Ember & Oak Kitchen - King of Prussia has the highest void rate this quarter, and what reasons are they giving?"
- Expect: Tyler Brennan ~11.6% vs ~2% for everyone else; most of his voids are MANAGER OVERRIDE or NO REASON.
- Say: "A manager's hunch to a case file in one question, on live data."

Ask: "Why did salmon sales at Ember & Oak Kitchen - Fishtown drop in early October?"
- Expect: no sales Oct 7-10; menu change log shows it was taken off (count 0) Oct 7 and restocked Oct 11.
- Say: "Demand didn't drop - the kitchen ran out. Two collections, one answer."

Ask: "How is the Nashville Hot Chicken Sandwich launch going at Ember & Oak?"
- Expect: launched ~Sep 19, weekly units still climbing (~1.6K -> ~3.7K); last week partial.

## 2. Three years of history (Analytics - seconds)
Ask: "Is this year's Pumpkin Cheesecake season at Ember & Oak better than the last two?"
- Expect: same weeks each year, 2026 ~+32% vs 2025.
- Say: "Two of those three seasons no longer exist in the operational tier - Analytics has all of it."

Ask: "How is card volume split across our payment processors, and is it growing?"
- Expect: ~$1B over 3 years, cardConnect ~88%, dejavoo ~12%; 2025 +32% vs 2024.

Ask: "How has the share of online and delivery orders changed over the last three years?"
- Expect: ~8% -> ~18% (Sep 2026), accelerating; October partial.

## 3. Back to the discovery question
- Walk through its method, the chain finding and its confidence notes. If it offers a weekly scheduled report, show the
  offer - that's AI moving from one-off analysis to operations.

## Rules
- Fresh chat every rehearsal and on stage. Never the working project.
- The findings are planted in synthetic data: "the method is real, the numbers are illustrative."
- If Claude picks the wrong King of Prussia location, re-ask with the full name (there are 9 in the data).
