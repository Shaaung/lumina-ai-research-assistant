# DESIGN.md — LUMINA

## Components

This project implementation uses the provided React UI, an Express Gateway, an Express Agent service, a background document worker, MongoDB Atlas, and external search and model APIs. The browser runs the UI on port 5173 and sends requests only to the Gateway on port 8787. The Gateway forwards application work to the Agent on port 8000. The Agent and its worker use the Atlas cluster `lumina-practice`, with the application database named `lumina`. Atlas stores application records and vectors. Tavily is the selected web-search provider. Quick synthesis and Deep planning use Anthropic Claude Haiku 4.5; Deep synthesis uses Claude Sonnet 5. OpenAI text-embedding-3-small supplies document and memory embeddings. The deployed UI is on Vercel and backend services run together in Fly.io Chicago (ord), near Atlas in GCP Iowa (us-central1).

## Responsibilities

The UI collects questions and displays plans, sources, streamed answers, memories, and document status. It never receives provider credentials. The Gateway is the only service exposed to the browser; it validates requests, applies request IDs and rate limits, and forwards SSE without buffering. The Agent owns the bounded Quick and Deep research loops, provider calls, citation construction, conversation and memory behavior, document retrieval, and run records. The document worker owns upload processing: parse, chunk, embed, index, and verify retrieval before marking a document indexed. Only private backend processes (the Agent and its document worker) may use provider or database credentials; the Gateway never receives them. Atlas is the durable store; its access roles and network access list control database access.

## Communication

The browser calls the Gateway over HTTP and receives progress over Server-Sent Events (SSE). The Gateway calls the Agent over the same HTTP contract and passes the event stream through without buffering. The Agent calls Tavily and the configured model over HTTPS and reads or writes Atlas through the MongoDB driver. The worker uses the embedding provider and Atlas, claims document jobs from the jobs collection, and updates their status there; the UI reads status through the API. If the Agent, Atlas, or a provider is unavailable, the request must finish as an explicit error rather than return a plausible answer. The Gateway reports provider failures to the browser as an error response. Deep mode emits its plan before retrieval; both modes emit sources before answer tokens.

## State

Atlas is authoritative for threads, messages, saved memories, spaces, documents, chunks, jobs, search-cache entries, and deployed run records. The local `runs/<requestId>.json` files are evidence consumed by the quality and evaluation tools; deployed run records are persisted to Atlas. Search results are cached in memory and in the Atlas `searchCache` collection with an expiry time. Either cache can be deleted and rebuilt without losing user data. Each document chunk keeps its text, embedding, and page locator together. A document is not searchable until the worker's read-your-write probe confirms that retrieval can find it. Local credentials stay in the ignored `.env`; deployed credentials belong in the hosting provider's secret store.

## Trade-offs

Atlas keeps records, chunks, embeddings and citation locators together, simplifying deployment but introducing Atlas-specific indexes and network access requirements. Separate Gateway and Agent services keep provider keys private at the cost of another transport hop and separately billed machines.

Quick reads up to three pages concurrently with a 600 ms network deadline and lighter text extraction. Deep plans three sub-questions with Haiku and attempts up to three pages per sub-question, with longer fetch deadlines and full extraction; Sonnet merges the answer. Shared URLs are deduplicated across sub-questions. Short Quick deadlines reduce waiting but increase explicitly disclosed snippet fallback; neither missing text nor an unresolved citation should be treated as evidence. A successful run can contain an explicitly failed optional page fetch when other retrieved evidence supports an answer.

Memory and document retrieval overlap independent work to reduce waiting. Uploads use a shared, pre-initialized GridFS bucket and a Mongo connection pool warmed with four concurrent read-only pings and a minimum of four connections. File and pending metadata storage overlap, then the durable job is inserted before returning 202. Startup becomes slower, while the worker still owns parsing and embeddings. A two-upload probe showed 263/248 ms acceptance, but does not establish full-load p95 or indexing performance.

The latest complete ready-service Chicago benchmark (2026-10-08T21:26:39Z–2026-10-08T21:33:47Z) failed two SLA targets: Quick first-token p95 2849 ms (2500 ms limit) and upload acceptance p95 3155 ms (300 ms limit). Quick answer p95 was 6262 ms. Deep plan p95 was 2518 ms, Deep answer p95 47.9 seconds, and minimum source ratio 2.33, all passing. Recall@5 was 0.90; grounding verified 140/140 citations, excluding 11 blocked/unverifiable citations, with zero dangling citations. All 90 new trajectories terminated done and request error rate was zero. Web searches were fully cached. Warmup does not eliminate intermittent end-to-end delays.

The provided UI is retained because web/ is immutable for this assignment. Its composer position and manual scrolling are known usability limitations. Fly machines are stopped between authorized tests to limit costs; they must be available throughout grading. The evaluation remains a draft until a 60–90 second demo is embedded, a human reviews both trajectories, and outstanding benchmark/quality failures are resolved.

### Performance refinements

- Deep planning uses `PLANNER_LLM_MODEL` (Haiku by default) to reduce plan latency and cost; Deep synthesis continues to use Sonnet. Plans must still validate against the contract and contain distinct, attributed sub-questions.
- The upload service reuses one GridFSBucket, retains a separate stream per upload, and creates a pending job only after GridFS storage and document metadata both succeed.
- `QUICK_PAGE_TIMEOUT_MS` defaults to 600 ms. Quick still attempts to fetch three pages; a timed-out page is logged as a failed fetch, and any search-snippet fallback is explicitly identified in the trace. This reduces waiting but may increase snippet use on slow publishers; grounding and answer quality require deployed verification.
- The five-minute page-text cache stores successful page text only. Cache use is disclosed in the trace and does not change the search-cache-hit metric.
- These refinements have not yet been validated in a new deployed benchmark. Existing failed measurements remain evidence, and SLA thresholds are unchanged.

### Stream reliability and Deep accounting correction

- The Anthropic reader completes on the provider's `message_stop` event, validates the terminal event and stop reason, and cancels/releases its reader on success or failure. Premature EOF is an upstream error, not a successful answer.
- The Gateway cancels its upstream request when the browser disconnects. Backpressure waiting uses the same abort signal as the request timeout, so a disconnected browser cannot leave a drain wait open indefinitely. Started requests are logged as well as completed ones; an upstream error after streaming starts emits an SSE error before closing.
- Deep synthesis explicitly passes its Sonnet model to usage accounting. Earlier Deep probe costs used the default Quick/Haiku rates and understate the cost; retain the old evidence, but do not use those totals as current billing estimates.
- Deep synthesis has a 3,000-token output ceiling instead of 2,200 after the third probe hit `max_tokens`. The 350-word prompt target, 24-tool/240-second hard caps and daily spend gate remain unchanged. The larger ceiling can increase paid API output usage; truncation still reports `terminated: cap` honestly.
- The interrupted Quick smoke run has no proven root cause. These changes address concrete stream lifecycle risks; a new deployed run is required to establish whether they resolve that interruption. No paid API calls or deployment were made for this correction.

### Document retrieval and intake latency refinement

- Quick document retrieval starts alongside memory recall. Both outcomes are awaited and failures retain failed tool traces; retrieval is not skipped or replaced by an ungrounded answer.
- Atlas vector and lexical pipelines execute concurrently, retain their user/Space/document filters, and feed the unchanged reciprocal-rank fusion. Returned Atlas rows omit the unused embedding arrays to reduce payload size; the cosine-scan backend still loads embeddings for scoring.
- The upload ownership lookup runs concurrently with multipart receipt and validation. Captured promise failures are handled explicitly. GridFS and pending document storage still finish before the indexing job is published, and all three durable records still finish before HTTP 202.
- Intake logs identify ownership, storage, job insertion and total acceptance times. The parallelism is expected to reduce serial waiting; no deployed timing improvement is claimed until a fresh benchmark measures it. No deployment or paid provider call was made for this refinement.

### Interrupted round-five verification

The round-five deployed smoke probe reached the Gateway ask handler but did not finish its first question before the 180-second test watchdog. The Gateway logged client cancellation; no speed improvement or SLA pass is inferred. The three Fly Machines were stopped after the interruption.

A local follow-up bounds MongoDB pool waits (5 seconds), connection establishment (10 seconds) and socket inactivity (15 seconds). Initial connection is shared and failures reset it for a later retry. Quick retrieval logs start, completion/failure and elapsed time per tool so the next probe can distinguish memory, search and document waits. These address an unbounded database-wait risk; the interrupted request's precise root cause is still unknown. This follow-up is not deployed and has not made paid provider requests.

### Waiting and cancellation protection

- The Gateway has a 30-second deadline for initial upstream response headers. A silent upstream returns an honest HTTP 502 with the deadline reason and request ID; no automatic paid retry is performed. Active-stream safety timers allow 105 seconds for Quick and 255 for Deep, leaving transport cleanup time beyond the Agent's declared 90/240-second gear caps.
- Request-scoped cancellation in the Agent combines caller disconnect with each provider timeout for embedding, search, page fetching, planning and synthesis. It cancels outstanding provider transports instead of letting work continue after the Gateway gives up. A cancellation cannot undo provider usage already billed.
- Agent logs distinguish receipt, thread lookup completion and individual retrieval stages. MongoDB waits retain the explicit bounds added after round five. These safeguards address waiting/cancellation failure modes; they are not proof of the earlier stall's root cause or of SLA performance.

### Waiting protection probe and citation follow-up

The bounded probe returned a terminal SSE error in approximately 6.5 seconds, rather than hanging: generated text referenced source [4] although only three sources were retrieved. This is a failed answer, not a successful benchmark or proof that all waiting failures are resolved. Paid testing stopped after that first failure.

The source page contained its own bracketed numeric footnotes. The local follow-up removes those markers only from model context and explicitly enumerates allowed source markers in the synthesis prompt. Original retrieved text/source snippets remain intact for grounding evidence; unresolved output citations still fail loudly. This prompt follow-up is not deployed or tested, and does not repair or relabel the failed answer.

### Bounded page text extraction

The extractor reads at most 1 MB of each fetched body and closes its reader. It removes non-readable payloads, prefers a declared main/article region, and excludes navigation/header/footer/sidebar/form markup before constructing a DOM of at most 120,000 body characters. Plain-text and Markdown responses bypass DOM construction. Readability, page-text cache and unresolved-citation validation remain in use; no extra provider request is introduced.

This reduces parsing work but large or unusual pages can have relevant content beyond the retained region or size limit. Sources/snippets remain tied to the text actually extracted, and insufficient evidence must not be replaced by invented facts. Debug extraction logs include parse duration and retained character count. The change has not been deployed or benchmarked; previous timing evidence remains unchanged.

### Quick block extraction

Quick page reads extract text-bearing paragraphs, headings, list items, preformatted blocks and table rows from the already selected article region. A small DOM fragment decodes entities; the existing full Readability path is used if too little text is found. Deep keeps the full extraction path. Mode-specific cache keys prevent Deep from reusing shorter Quick extracts. Extraction duration is now logged at info level for deployed diagnosis.

The lighter path is expected to reduce DOM CPU work but can miss text in unusual layouts or late blocks. Retrieved source snippets remain tied to actual extracted text, and citation validation is unchanged. No speed, grounding or SLA improvement is claimed without new deployed evidence; this change has not been deployed or tested.


### GridFS readiness before first document upload

The full deployed benchmark on 2026-10-08 measured upload acceptance p95 at 375 ms against the required 300 ms. Later corpus uploads took 241–244 ms. The first-use GridFS index check is a candidate contributor, not a confirmed root cause; that benchmark did not retain a stage-level intake trace.

At Agent startup, the shared bucket now performs the MongoDB driver's index readiness check via its public INDEX event. A construction-only stream is destroyed after readiness; no file bytes are written and the stream is never ended, so it creates no placeholder file, document, or indexing job. Actual uploads share the readiness promise. Failure or timeout resets the bucket and is logged; upload storage failures still return 502. Each intake log additionally records preparationMs.

The endpoint still waits for completed GridFS storage and pending document metadata before inserting the pending job, then waits for that insert before returning 202. Parsing, embeddings and indexing remain on the worker. This local change has not been deployed, typechecked, or tested; the saved 375 ms benchmark remains the current evidence. Startup preparation can overlap the first real upload, so its latency remains unproven until a deployed first-upload measurement.


### Deployed upload readiness probe — 2026-10-08

Agent deployment 01M4ECV0XXRV325G5DGBXFRDY7 passed TypeScript checking. Ten deployed corpus uploads all returned HTTP 202 pending. The first took 589 ms; the other nine took 234–286 ms. The nearest-rank p95 across all ten is 589 ms, so this probe did not pass the 300 ms target. It does not replace the saved official full benchmark.

Startup readiness took 809 ms and completed before uploads. The first intake reported ownershipMs 55, preparationMs 0, storageMs 437, jobMs 50, acceptMs 542 (server timing). Later storage stages took 108–115 ms. Index preparation alone therefore did not resolve the first-upload delay; the file/metadata persistence stage needs finer measurement. Network timing accounts for the difference between server and client measurements.

Worker stayed stopped; no parsing, embeddings or indexing were exercised. All ten temporary GridFS files and this isolated user's documents, jobs and Space were removed before services stopped. Agent and Gateway stopped at 18:38:18 UTC; Worker remained stopped. No model API was called in this probe. See reports/upload-readiness-probe.json for request IDs and stage evidence.


### Separate persistence timing for upload diagnosis

Intake logs now measure gridFsMs (the GridFS stream through completion), documentInsertMs (pending document insert), and multipartMs (multipart receipt and validation elapsed at the persistence boundary). Both persistence timers are recorded on settlement, including failure. Failed intake logs carry the requestId and available persistence times so Gateway and Agent records can be correlated without logging file contents or credentials.

GridFS and metadata insertion still run concurrently; their durations overlap and must not be added together. storageMs covers readiness plus the parallel persistence stage; jobMs remains the subsequent durable job insert. The measurements include database pool wait and network round trips; they cannot independently identify Atlas processing time. This instrumentation is local and has not yet produced deployed measurements or an improved SLA result.


### First-upload persistence diagnosis and pool readiness follow-up

The deployed two-upload probe on 2026-10-08 returned HTTP 202 pending in 582 ms then 292 ms (client measured). First intake: ownership 54 ms, preparation 0 ms, GridFS 444 ms, pending-document insert 54 ms, job insert 51 ms. Second intake: GridFS 118 ms and document insert 51 ms. This isolates the extra first-use delay to the GridFS path rather than the metadata insert; it does not prove the delay came from connection setup rather than a server/write effect.

The next local change warms three primary MongoDB connections using concurrent read-only ping commands before shared db() readiness resolves. This targets a cold additional socket when GridFS and metadata writes overlap. Unlike merely setting minPoolSize, it awaits completion of real overlapping operations. No synthetic upload, placeholder file, document or job is created. Startup takes longer; writes still await acknowledgment and errors still reset/close the client and propagate. This follow-up is not deployed or measured; the first-upload SLA is still failed in current evidence.

Both test files and the isolated test user's documents, jobs and Space were removed. Agent and Gateway stopped at 18:46:59 UTC; Worker stayed stopped and no model or embedding API was called. The measured run is preserved in reports/upload-persistence-timing-probe.json.


### Deployed connection warm-up probe — 2026-10-08

The connection warm-up follow-up was deployed and two isolated corpus uploads returned HTTP 202 pending in 263 ms (first) and 248 ms (second), both below 300 ms. These results support the cold-connection hypothesis, but do not uniquely establish it or prove full-load p95 compliance. This two-upload probe does not replace reports/bench.json or the official failed upload p95 of 375 ms.

Stage timings and image identity are saved in reports/upload-pool-warmup-probe.json. Worker remained stopped, so no model or embedding API was called and document indexing was not tested. Both temporary files, documents, jobs and the isolated Space were removed. Agent and Gateway stopped by 18:50:46 UTC after approximately 30 seconds of service runtime. A full deployed benchmark remains necessary to update the official /evals evidence.


### Evaluation evidence scope and draft status

The grading trajectory set is the entire 90-run deployed full session between 18:14 and 18:23 UTC on 2026-10-08, including memory and daily-cap probes. Earlier implementation/probe records were archived as a complete set of 39 runs (24 done, 11 error, 4 cap), not filtered by outcome. They remain in runs/history-before-full-20261008; the archive manifest and previous quality/evaluation drafts remain in reports. The deliberately invalid-key trajectory remains in runs/failing. Current full-run error rate and the failed official upload p95 were not changed.

Quality checking on that complete current workload reports zero errors and one warning. P2 identifies TODO precedents in the provided immutable quality/rules.json; this warning is retained. P1 requires the student to read successful req_365fabe8-c59 and failing req_eval_invalid_key end to end and confirm the reflections. Agent review is not substituted for human completion.

The student confirmed there is no demo video yet and requested a draft. A proposed 75-second recording: 0–10s introduce the app and Quick/Deep choices; 10–30s show an existing cited Quick answer and trace; 30–45s show an existing Deep plan and attributed sources; 45–60s show indexed documents and memory controls; 60–75s show /evals, explain one failure and current limits. Recording live new questions or uploads can incur provider costs; displaying existing results avoids those calls. A publicly viewable 60–90 second video URL still needs to be supplied and embedded.


### Full deployed benchmark after connection warm-up

The complete 2026-10-08 19:05–19:13 UTC session passed upload acceptance p95 at 262 ms, but failed Quick first-token p95 at 2657 ms (target 2500 ms). Quick answer p95 was 6110 ms; Deep plan p95 2667 ms and answer p95 about 31.4 seconds. Recall was 27/30. Grounding was 143/143 verifiable citations, with 13 blocked/unverifiable citations excluded by the provided checker; this is not a claim that every cited page was verified. No dangling citations or request errors occurred. The repeat web workload was entirely cached and does not establish uncached search latency.

All 90 new persisted trajectories finished done and remain alongside the preceding full session's 90 records. Quality checks therefore read all 180 full-session trajectories, not an outcome-selected subset. Historical probe/version records remain archived. The new full benchmark and earlier reports are both preserved; no benchmark numbers were hand edited.

Model cost summed from the 90 new run records was USD 0.67723224, excluding document-embedding, search and Fly charges. Agent, Worker and Gateway stopped by 19:13:32 UTC after approximately 7 minutes 40 seconds. No automatic retry or extra paid run followed. The refreshed local evaluation report is still a draft without a video or completed human review, and has not been redeployed; the public page retains its prior report until the next deployment.


### Quick retrieval overlap and first-token timing

The current full benchmark misses first-token p95 by 157 ms. Page reads now start as soon as web_search resolves, overlapping unfinished recall_memory work. Search time excludes page fetches. The same three fetches, deadlines, error traces, source content, citation checks and sources-before-tokens sequence remain; no additional provider call is added. Synthesis awaits both memory and pages. This helps only when recall outlasts search, and does not guarantee provider latency.

Server logs now split retrievalReadyMs, modelHeadersMs, streamFirstTokenMs, modelFirstTokenMs and actual ttftMs. No placeholder token is used to improve the metric. The change is local, undeployed and unmeasured; existing failed benchmark results remain unchanged.

### Deployed Quick overlap diagnostic — 2026-10-08 19:23 UTC

Deployed Agent image `deployment-01M4EFEFF0ANK5EF1QQJGJP89Q` and ran four sequential Quick web requests using unchanged benchmark questions. All four terminated as done, emitted sources before tokens, and used the Tavily search cache. Client TTFT was 1561, 1187, 1210, and 834 ms; complete-answer latency was 4299, 4823, 3907, and 4345 ms. Server retrieval readiness was 865, 768, 758, and 385 ms; model first-token time after retrieval was 656, 392, 363, and 396 ms. Evidence: `reports/quick-overlap-diagnostic.json` includes full answers, traces, and timing logs.

This is a small sequential diagnostic, not proof of the full concurrent-workload p95 SLA. Preserve the full benchmark TTFT failure (2657 ms) and generated eval score until a new official full benchmark measures it. No full report metrics were overwritten. Measured answer-model cost totaled USD 0.012416, excluding recall embeddings and Fly charges. Agent/Gateway were started around 19:23 UTC and both stopped by 19:23:50 UTC; Worker remained stopped. Testing took about 18 seconds; the application machines were enabled for roughly one minute including startup and log collection.


### Full deployed benchmark after Quick overlap — 2026-10-08

The latest complete ready-service Chicago benchmark (2026-10-08T21:26:39Z–2026-10-08T21:33:47Z) failed two SLA targets: Quick first-token p95 2849 ms (2500 ms limit) and upload acceptance p95 3155 ms (300 ms limit). Quick answer p95 was 6262 ms. Deep plan p95 was 2518 ms, Deep answer p95 47.9 seconds, and minimum source ratio 2.33, all passing. Recall@5 was 0.90; grounding verified 140/140 citations, excluding 11 blocked/unverifiable citations, with zero dangling citations. All 90 new trajectories terminated done and request error rate was zero. Web searches were fully cached. Warmup does not eliminate intermittent end-to-end delays.

Evidence is preserved in `reports/bench-full-overlap.json`, `reports/eval-full-overlap.json`, and `reports/quality-full-overlap.json`. All 270 trajectories from the three full sessions remain included in quality checks: zero errors and one warning from immutable provided rule precedents. Earlier probe and failure evidence is retained. The diagnostic four-question test is separate from the full benchmark.

Machine test window: 19:27:58–19:35:25 UTC (7 minutes 27 seconds including startup and shutdown). All three machines were confirmed stopped. Recorded answer-model cost for 90 new runs: USD 0.69680924, excluding embeddings, search and infrastructure. This is still a draft without a 60–90 second demo video and without confirmed human trajectory review. The local report is regenerated by the official builder; it is not yet deployed.


### Upload connection headroom and relay timing — local refinement

The latest full benchmark missed upload acceptance by 23 ms (323 ms versus 300 ms). Its per-upload stage logs are no longer available in the recent Fly log buffer, so the specific outlier cannot be attributed to connection creation. Earlier diagnostics showed ownership/job calls near 50 ms and GridFS storage near 110 ms after warm-up, with a 444 ms first-write outlier before pool warm-up.

Keep a minimum of four MongoDB connections per service and allow four concurrent connection handshakes. Await four read-only primary pings during initial connection readiness. Unlike pings alone, the explicit minimum asks the driver to maintain connection headroom for the existing four-request concurrent workload. This can reduce connection checkout/handshake stalls, but does not remove Atlas round-trip latency or guarantee that the 323 ms outlier is fixed. Idle database connections consume Atlas connection capacity; this does not increase Fly machine resources or invoke model APIs.

Add Gateway logs for upload-to-upstream-response, response parsing, and total relay handling time; correlate with Agent stages using the same request ID. Ownership remains a fresh database lookup; durable GridFS storage and document metadata must complete before publishing the worker job, and that job must be acknowledged before returning 202. No storage acknowledgment or grading threshold is relaxed. Changes are local until deployed and measured.


### Deployed upload pool diagnostic — 2026-10-08 20:19 UTC

Agent image `deployment-01M4EJKFZYH2A13237E9YCWX2T` and Gateway image `deployment-01M4EJN6SWPKPBZPJ36GFNG2S5` are deployed. Four sequential corpus uploads returned 202/pending in 330, 237, 295 and 234 ms at the client. Agent acceptance was 221, 215, 212 and 215 ms; Gateway relay handling was 227, 220, 215 and 220 ms. The client-minus-Gateway differences were 103, 17, 80 and 14 ms. They include transport, request receipt and timing-boundary overhead outside the route handler; these measurements do not establish a specific network root cause for the previous full-benchmark 323 ms result. All four acknowledged file/metadata/job storage; the Worker remained stopped, so parsing, embeddings and indexing were not tested. All isolated test files, jobs, document records and the test Space were removed. Evidence: `reports/upload-pool4-diagnostic.json`.

One sample still exceeds 300 ms. This diagnostic cannot replace the complete benchmark or establish upload SLA compliance. Full report metrics remain Quick TTFT p95 2085 ms and upload p95 323 ms. The latest generated full report was deployed with Gateway and its public JSON endpoint returned 200 with those exact metrics; the report's generation-time note about not being redeployed is now a historical snapshot. Machine startup-to-shutdown window was 20:19:09–20:19:21 UTC (12 seconds). All three machines were confirmed stopped. No model or embedding calls were made; Fly deployment/build/runtime costs are separate and were not measured here.

### Upload transport diagnostic — 2026-10-08 20:27 UTC

Four sequential uploads returned 202/pending in 298, 271, 408 and 289 ms at the client. Undici diagnostics recorded sendHeaders on the same HTTPS socket for all four, with no new beforeConnect/connected events during those requests. This rules out client-side connection establishment as the cause of the 408 ms sample in this diagnostic; it does not identify every component of the wider network path. Gateway handling was 238, 228, 217 and 223 ms respectively. The slowest client's extra 191 ms occurred outside the Gateway handler timing boundary, which includes client/transport/proxy timing and is not attributable to a specific network component from these logs alone.

Do not change keep-alive settings solely to explain this outlier: these four requests already reused their connection. Next investigate Atlas/Fly region proximity and reduce the approximately 200 ms backend baseline while preserving fresh ownership checks and acknowledged file/document/job storage. Region changes require verifying the actual Atlas location and considering relocation costs before modifying deployment. The formal full benchmark remains failed on upload p95 323 ms; this diagnostic does not overwrite it.

Evidence: `reports/upload-transport-diagnostic.json`. Worker was never started, no model/embedding calls were made, and the four test files plus their isolated jobs/documents/Space were cleaned up. Machine startup-to-shutdown window: 20:26:45–20:27:08 UTC (23 seconds). Agent, Gateway and Worker were all confirmed stopped. Fly infrastructure costs were not measured.


### Verified deployment-region review — 2026-10-08

MongoDB Atlas's authenticated Clusters page shows lumina-practice on the FREE tier in GCP Iowa (us-central1). All three Fly configuration files specify Los Angeles (lax), matching the existing machines used in diagnostics. This is a cross-region database path. Chicago (ord), listed in Fly's official region reference, is a candidate for a closer backend placement; this is an inference from location, not a measured latency improvement.

Next compare a temporary backend placement near Atlas with the existing deployment using a small upload-only workload, retaining the current machines stopped as a rollback option and keeping the same database. The comparison must include client-to-Gateway latency as well as database stages: moving the backend closer to Atlas can increase distance from a West Coast client. Do not migrate the Atlas cluster, recreate indexes, or discard data merely to test placement. Any temporary machines introduce separate Fly compute/transfer costs and must be stopped after the comparison. No region was changed in this review and no machine/model/embedding call was started. Official full benchmark metrics remain unchanged.


### Measured ORD/LAX comparison — 2026-10-08 20:37 UTC

Temporary Agent and Gateway clones in Chicago (ord) were tested against the unchanged Iowa Atlas database, using the same four corpus files as a subsequent Los Angeles (lax) comparison. Requests were pinned to each Gateway machine with Fly's documented request routing header. Agent/Gateway log hostname and region were checked for all four upload request IDs in each region. No worker was started.

Client acceptance in ORD: 194, 150, 173, 193 ms (mean 177.5 ms). LAX: 239, 247, 230, 236 ms (mean 238 ms). ORD Agent durable-intake timing: 67, 61, 60, 56 ms; LAX: 214, 225, 212, 216 ms. Gateway timing: ORD 74, 65, 64, 62 ms; LAX 224, 231, 216, 220 ms. Closer-to-Atlas backend placement reduced the measured server processing baseline substantially, while the West Coast client's added transport distance offset part of that benefit. Client mean improved about 25% in this small comparison. These runs were sequential, ORD first then LAX, and not randomized or concurrent; do not infer full-workload p95 compliance, model performance, or indexing speed from them.

Evidence: `reports/region-upload-comparison.json`. All eight isolated test uploads and their metadata/jobs/Spaces were cleaned up. Temporary ORD clones were stopped and destroyed after testing; original LAX machines were retained and stopped. The active deployment configuration is still LAX. No provider model or embedding calls occurred; Fly infrastructure costs apply and were not measured. Formal full benchmark/report metrics remain unchanged, including upload p95 323 ms.

Recommended next step: deploy the persistent Agent/Gateway/Worker together in ORD using unchanged images and resource sizes, retaining stopped LAX originals until a new complete benchmark validates Quick/Deep/upload/indexing behavior. Moving only one component would add an inter-region private hop. Backend internal service names and the existing public Gateway URL can remain stable; preserve Atlas data and indexes. Region placement must be reflected in the Fly configs when that migration is authorized.


### Authorized ORD deployment — 2026-10-08

The user authorized placing Agent, Gateway and Worker together in Chicago (ord) after the small comparison. Keep the original LAX machines stopped for rollback, and cordon the LAX Gateway so the public proxy cannot wake it or route requests to it. Update all three Fly configs to ord. Create persistent ORD machines with the existing resource sizes; the ORD Worker should use the current Agent image containing the latest shared database/worker code and its worker process command. No Atlas cluster, data, indexes, provider keys or public URL changes are required.

Deployment verification checks the public Gateway health/DB result, observed machine regions and Worker startup. The jobs queue was read-only checked before worker start: zero pending/running document jobs. Do not upload a file or issue a model question during this deployment-only verification. Stop all machines after verification; ORD Gateway retains its prior auto-start policy, so visiting the app may wake it. Future deploy commands must target ORD machine IDs with --only-machines, preserving stopped LAX rollback copies. Full SLA results remain the preceding benchmark until a complete ORD benchmark measures performance.


### ORD deployment completed — 2026-10-08 20:43 UTC

Persistent Chicago machines: Agent 83d1542b701248; Gateway e820217f659668; Worker 84ed16ef474168. Agent and Worker use current image deployment-01M4EJKFZYH2A13237E9YCWX2T; Gateway uses deployment-01M4EJN6SWPKPBZPJ36GFNG2S5. Public Gateway /health returned HTTP 200 and db ok; region/hostname startup logs confirmed all three services in ORD and Worker startup. No files or model questions were submitted. The document job queue contained zero pending/running jobs before Worker startup. This validates deployment/connectivity, not provider API billing readiness, indexing or complete SLA compliance.

All six retained machines are stopped. Original LAX Gateway 85d10da4415338 is cordoned to prevent public routing/autostart; LAX Agent 813422f95e5558 and Worker 843e17c2590658 remain stopped rollback copies. Public Gateway URL, Vercel URL and Atlas data/indexes are unchanged. ORD Gateway retains auto-start behavior: visiting the app can wake Gateway and incur Fly cost; Agent/Worker still need explicit startup for functional tests. Stopped rollback copies may still incur provider storage charges and are not a promise of zero cost.

Deployment/startup/shutdown window was 20:42:22–20:43:29 UTC (about 68 seconds), with no requested model/embedding calls. Evidence: reports/ord-deployment.json. Future deployments should use --only-machines with the ORD ID and the relevant config so rollback copies remain untouched. A complete ORD benchmark is still needed; generated eval metrics continue to describe the earlier LAX session.


### Complete ORD benchmark — 2026-10-08 20:52 UTC

The latest complete ready-service Chicago benchmark (2026-10-08T21:26:39Z–2026-10-08T21:33:47Z) failed two SLA targets: Quick first-token p95 2849 ms (2500 ms limit) and upload acceptance p95 3155 ms (300 ms limit). Quick answer p95 was 6262 ms. Deep plan p95 was 2518 ms, Deep answer p95 47.9 seconds, and minimum source ratio 2.33, all passing. Recall@5 was 0.90; grounding verified 140/140 citations, excluding 11 blocked/unverifiable citations, with zero dangling citations. All 90 new trajectories terminated done and request error rate was zero. Web searches were fully cached. Warmup does not eliminate intermittent end-to-end delays.

All 90 new persisted trajectories are exported alongside all earlier complete sessions; quality reads all 360 trajectories, with zero errors and one immutable P2 precedent warning. Earlier failed reports and probe evidence remain preserved. The provided benchmark computes its accept202 capability after the four corpus uploads, then later appends the large-file upload to its final SLA p95; therefore the corpus capability passes while the final upload SLA fails. Preserve both original outputs and disclose the difference rather than modifying the grader.

Evidence: reports/bench-full-ord.json, reports/eval-full-ord.json, reports/quality-full-ord.json and reports/full-ord-session.json. Machine testing window 20:45:12–20:52:08 UTC (6 minutes 56 seconds including startup/shutdown). All six ORD/LAX machines were confirmed stopped. Recorded answer-model cost USD 0.70001824 for the 90 new runs excludes embeddings, search and Fly. This remains a draft without a demo video and confirmed human review. Focus the next investigation on the 60-page upload and slow Deep plan, rather than repeating the complete paid benchmark before diagnosing them.


### ORD SLA diagnosis — local instrumentation, not a new benchmark

The latest full ORD benchmark measured a 5,528 ms client-side Deep plan outlier, but the matching SSE/WebSockets/long-polling run (`req_2bb56dcd-155`) recorded only 2,129 ms in `plan_research`. These measure different boundaries; provider planning alone does not explain the client delay. Added request lookup/history timing, Deep allowance/planner/plan-send timing, and gateway upstream-header/first-frame timing to correlate by requestId. No query text or secrets are added to these timing logs. The cause of the remaining delay is still unconfirmed.

The synthetic 60-page benchmark PDF is only 24,638 bytes, below GridFS's default 261,120-byte chunk size. Increasing the chunk size cannot reduce its chunk count. Existing upload intake and gateway relay timings must be correlated with client latency on the next deployed test before changing persistence behavior. The file, pending document, and durable job must all exist before returning 202.

These instrumentation changes are local and have not been deployed or measured. The latest 321 ms upload and 5,528 ms Deep-plan failures remain in the official report. No Fly machines were started and no paid provider calls were made for this investigation.


### Deployed ORD timing diagnostic — 2026-10-08

Instrumentation was deployed to the ORD Agent and Gateway only. Two Deep diagnostic requests produced plans in 2,251 and 2,171 ms at the client; Agent plan-send times were 2,171 and 2,021 ms, and Gateway first-frame times were 2,183 and 2,027 ms. These two samples do not replace the full benchmark p95. Both requests completed successfully; reported model cost totaled USD 0.101830, excluding embeddings, search and infrastructure.

Four synthetic 60-page PDF uploads returned pending/202 in 192, 8,112, 266 and 150 ms at the client. The 8,112 ms outlier (`req_e032809c-fda`) had only 81 ms Agent intake and 89 ms Gateway request processing. This localizes most of that delay outside the measured request handlers; the precise transport/proxy cause remains unconfirmed. Worker stayed stopped, and all four temporary files, documents, jobs and the probe Space were deleted.

Deployment/test window: 20:59:36–21:01:47 UTC (131 seconds, including builds). Agent application startup was 21:00:45 UTC and Gateway startup 21:00:48 UTC; active application window was approximately one minute. All six ORD/LAX service machines were verified stopped afterward. Full diagnostic evidence is in `reports/ord-timing-diagnostic.json`. The generated full ORD report was bundled into the Gateway deployment; its failed full-run SLA measurements have not been replaced with these small diagnostic samples.


### ORD upload transport follow-up — 2026-10-08

Four warm synthetic 60-page PDF uploads returned pending/202 in 188, 211, 238 and 140 ms. Each reused the same HTTPS socket, sent its body within 1–10 ms, and completed without starting the Worker. All four probe uploads and their scoped jobs, documents and Space were removed. No model or embedding API calls were made. These samples do not replace full-benchmark p95 or prove the earlier 8,112 ms outlier fixed.

The initial health request established its connection in 52 ms, sent at 21:04:06.434 UTC, and received response headers at 21:04:18.371 UTC. Fly ingress recorded approximately 21:04:06.477 UTC, while Gateway processing took 249 ms. Most of the wait occurred after Fly ingress but before measured Gateway request handling. Startup/routing delay is a plausible explanation; its exact cause and relationship to the earlier upload outlier are unconfirmed. Changing GridFS or model limits would not address this observed boundary.

The test window was 21:03:58–21:04:24 UTC (26 seconds). All six service machines were verified stopped afterward; Fly infrastructure usage may still incur charges. Evidence: `reports/ord-upload-transport-diagnostic.json`. No production configuration was changed. Before a future timed test, explicitly start the backend machines and verify readiness so cold-start wait is measured separately; keep reporting any remaining upload SLA failures. Keeping machines continuously warm would incur ongoing compute cost and has not been enabled.


### Ready-service upload/indexing and Deep test — 2026-10-08

Explicitly started all three ORD services and checked public health three times before testing. The first health request took 6,160 ms; subsequent requests took 286 and 108 ms, all with DB ok. After these checks, the 24,638-byte synthetic 60-page PDF returned pending/202 in 211 ms (`req_d6f24456-133`) and became indexed with 60 pages/60 chunks in 4,310 ms measured from acceptance until polling observed completion. This includes polling delay and is not an exact worker execution time.

One Deep web request (`req_2ab5315b-3de`) produced its 3-question plan in 2,749 ms and completed in 18,383 ms client-side. Reported model/recall cost was USD 0.047085, excluding the PDF's embedding cost, search-provider credits and infrastructure. Search was cached. Both measured requests met their relevant latency thresholds, but this is a targeted sample, not proof of full-workload p95 or a replacement for the failed full benchmark.

Session: 2026-10-08T21:23:20Z to 2026-10-08T21:24:03Z (43 seconds, including startup and stop). All six ORD/LAX machines were confirmed stopped. The Worker was stopped before removing the temporary PDF, chunks, document, job and Space; the successful Deep run was preserved. Evidence: `reports/ord-ready-service-diagnostic.json`. No production code/configuration changes were needed for this test. Further latency claims require a full deployed benchmark with cold-start and ready-service boundaries disclosed.


### Complete ready-service benchmark — 2026-10-08

The latest complete ready-service Chicago benchmark (2026-10-08T21:26:39Z–2026-10-08T21:33:47Z) failed two SLA targets: Quick first-token p95 2849 ms (2500 ms limit) and upload acceptance p95 3155 ms (300 ms limit). Quick answer p95 was 6262 ms. Deep plan p95 was 2518 ms, Deep answer p95 47.9 seconds, and minimum source ratio 2.33, all passing. Recall@5 was 0.90; grounding verified 140/140 citations, excluding 11 blocked/unverifiable citations, with zero dangling citations. All 90 new trajectories terminated done and request error rate was zero. Web searches were fully cached. Warmup does not eliminate intermittent end-to-end delays.

The four corpus uploads returned pending/202 in 144, 3155, 196 and 197 ms, and all became indexed through the Worker. The upload capability and final latency SLA both fail in this run. Background-ingest search/idle ratio was 0.709. Quality checked all 450 complete-session trajectories: zero errors and one unchanged P2 precedent warning in the provided grader. Daily Deep cap and memory save/recall/delete checks passed.

Recorded answer/model cost for the 90 new runs was USD 0.68746924, excluding embeddings, search and Fly. Testing/start-stop window was 428 seconds; all six service machines were verified stopped. Cold startup health was 12118 ms; subsequent readiness checks were 102 and 183 ms before the benchmark began. The full-run failures remain despite warmup, so the single-request passing diagnostics are not evidence of stable p95 compliance.

Evidence: `reports/bench-full-ready.json`, `reports/eval-full-ready.json`, `reports/quality-full-ready.json`, `reports/full-ready-session.json`. Historical failed results remain preserved. Demo video and confirmed human trajectory review are still pending.


### Local latency fixes after ready-service full benchmark

Quick page-fetch traces include 2–3 second operations despite a 600 ms network timeout. The timeout cannot interrupt synchronous DOM parsing. Removed Quick's full JSDOM/Readability fallback: pages without conventional paragraph/list blocks now use bounded cleaned-text extraction with at most 18,000 characters of entity decoding. Deep still uses Readability. Empty/insufficient Quick page text remains a failed fetch with the existing explicit snippet-fallback trace; no text is fabricated. This reduces one source of shared-event-loop work, with a trade-off of less precise article extraction on nonstandard pages.

Gateway multipart forwarding now preserves a valid inbound Content-Length because the stream is relayed unchanged. Requests without that header retain streaming behavior. File, metadata and durable job persistence still finish before 202; parsing and embedding remain on the Worker. This avoids unnecessary chunked framing on the private hop, but is not evidence that Fly's intermittent multi-second routing delays are fixed.

Added approximate Fly-ingress-to-Gateway-entry timing to request/relay logs, filtered to plausible nonnegative values. It uses the externally supplied Fly timestamp, so clock differences may affect it and it never replaces end-to-end SLA measurements. Changes are local, not deployed or benchmarked. Existing failed full results remain unchanged; no model calls or Fly startups were requested for this edit.


### Deployed lightweight Quick / upload relay check — 2026-10-08

Deployed the local Quick extraction and upload forwarding changes to ORD Agent/Gateway. Four concurrent Quick requests completed successfully with client first-token times 2,089, 2,241, 2,397 and 2,322 ms (all below 2,500 ms), and answer times 5.36–6.36 seconds. No emitted citation marker lacked a matching source number; this structural check is not an independent semantic grounding grade. Recorded answer-model cost totaled USD 0.013655. Four samples cannot establish full-workload p95 compliance.

The four synthetic 60-page uploads returned pending/202 in 282, 184, 420 and 429 ms. The two slow requests had Agent intake 91/60 ms, Gateway relay 97/65 ms, and approximate Fly-ingress-to-Gateway-entry waits of 280/307 ms. The Gateway now forwards Content-Length, but the client upload target still fails on these samples. The newly measured delay is before application handling; further persistence or chunk-size tuning would not remove it. Maintaining more continuously warm capacity could incur ongoing costs and has not been enabled. No production configuration or grading thresholds were changed.

Worker stayed stopped, and all four temporary files/documents/jobs/Space were cleaned. All six service machines were verified stopped afterward. Deployment/test window 2026-10-08T21:58:15Z–2026-10-08T21:59:42Z (87 seconds including builds). No document embedding calls were requested; search-provider credits and Fly costs are separate from the recorded model cost. Evidence: `reports/latency-fix-diagnostic.json`. The full benchmark/report remains the previous failed run; these diagnostic samples do not replace it.


### New-network diagnostic — 2026-10-08

A read-only local Atlas ping succeeded after the network change; no credential/IP change was required. Reused the deployed latency-fix version without redeployment. Four concurrent Quick requests finished done without unresolved citation numbers, but client TTFT was 3381, 3342, 5502 and 4444 ms: all exceed 2500 ms. Corresponding Agent TTFT included 3050, 2969, 5391 and 1034 ms, so the fourth case also shows substantial delay outside Agent processing. Retrieval-ready times reached 4885 ms on another case. The earlier small passing sample does not establish stable compliance.

PDF acceptance times were 220, 577, 235 and 1963 ms. The slowest request had approximate Fly ingress wait 1806 ms and Gateway relay 63 ms; Agent intake was 58 ms. Two uploads exceeded the 300 ms target. These observations do not identify a single network root cause.

Session 2026-10-08T23:52:24Z–2026-10-08T23:52:50Z, 26 seconds including startup/shutdown. Recorded four-answer cost USD 0.012882, excluding search credits and Fly. Worker remained stopped, all temporary uploads were cleaned, and all six service machines were verified stopped. Full report scores remain unchanged. Evidence: `reports/new-network-diagnostic.json`.


### Search-cache startup and configuration correction — local

The new-network diagnostic had three uncached searches: web_search took approximately 1.76, 1.77 and 3.77 seconds before page fetches. A cached fourth query searched in 17 ms but had substantial client/transport delay. These are distinct boundaries; page extraction changes cannot remove uncached provider time or Fly routing wait.

Corrected the Agent numeric environment parser so missing/blank values use the documented defaults; an intentional numeric zero remains zero. Previously Number('') produced zero, which could unintentionally disable cache lifetime or timeouts if corresponding settings were blank. This is a configuration defect found in code, not proof that the deployed TTL was blank. No explicit nonblank TTL or billing limits were changed.

Prepare the search TTL index at startup using the existing shared readiness promise; searches still await/retry readiness and fail explicitly if preparation fails. Added cache-hit lookup, provider response/body and cache-persistence timing without logging queries or credentials. Quick cleaned text skips DOM entity decoding when no entities exist; Deep extraction is unchanged. These changes are local and unmeasured, not a claim that uncached TTFT or upload p95 now passes. No Fly startup or paid API calls were requested for this edit.


### Deployed cache correction diagnostic — two questions repeated

Deployed the Agent cache/startup/decoder correction and asked two distinct Quick questions twice. First-round searches were both uncached: client TTFT 3934 and 3435 ms. Second-round searches both hit cache: TTFT 1057 and 594 ms. All four answers finished done, with no unresolved citation numbers. This structural check does not establish semantic grounding or a full-workload SLA.

Deployed startup confirmed TTL 21600 seconds and index readiness in 1038 ms. Uncached Tavily response/body took 2253 and 2107 ms; persistence took 23 and 17 ms. Cache correctness is confirmed for these samples, but provider time plus fetched-page/model time still exceeds the uncached TTFT target. This is not a claim that the earlier deployment had a blank TTL, nor a reason to increase TTL solely to make the benchmark pass.

Session 2026-10-09T00:03:20Z–2026-10-09T00:04:50Z, 90 seconds including deployment. Recorded four-answer cost USD 0.014147, excluding search credits and Fly. Worker remained stopped and all six service machines were verified stopped afterward. Evidence: `reports/cache-fix-diagnostic.json`. Official full benchmark/report remains unchanged; continued claims of improvement must distinguish cached from uncached workloads.


### Parallel thread/history reads — local

Thread ownership and the most recent six messages now load concurrently from the same connected database, with userId in both filters. Provider work begins only after ownership is confirmed; unknown or foreign threads still return 404, and database failures remain explicit 502 responses. This removes one sequential read round trip without dropping conversation context.

The cache-write/page-fetch overlap proposal was not implemented: measured cache writes were only 17–23 ms, and starting tools before durable cache completion would require additional failure-trace handling. Retaining the current explicit failure behavior avoids an unmeasured structural change for a small potential saving. This optimization does not remove Tavily or Fly wait, is local and not deployed, and has not been used to revise benchmark scores. No paid provider calls or Fly startups were requested.


### Normal versus forced-instance upload routing diagnostic

Compared eight identical synthetic 60-page uploads in interleaved order (normal/forced/forced/normal/forced/normal/normal/forced), four per mode. Normal acceptance: 280, 180, 310, 164 ms (mean 233.5 ms, one above 300 ms). Forced ORD instance via fly-force-instance-id: 231, 211, 178, 204 ms (mean 206 ms, all four below 300 ms). All requests returned pending/202, and Gateway logs identify the same ORD machine in both modes. The header selects an instance through the public Fly edge; it does not bypass that edge.

These small samples show a difference on this run, not statistical evidence of sustained p95 compliance or a fix for earlier multi-second ingress waits. No production routing setting was changed, no grader header was injected, and previous failed full measurements remain in the official report. Evidence: `reports/upload-route-comparison.json`.

Session 2026-10-09T00:22:20Z–2026-10-09T00:22:37Z (17 seconds including startup/shutdown). No model or embedding requests; Worker stayed stopped. All eight scoped upload files, documents, jobs and the probe Space were removed. All six machines were verified stopped afterward. Fly usage charges may still apply.
