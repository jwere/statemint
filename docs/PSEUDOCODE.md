# StateMint: Pseudo code

Nov 4, 2026 · @Lucid Technologies

The core logic of every stage, with edge cases written in, to be read alongside the architecture doc.

## Conventions

The pseudo code below is language-neutral but maps directly to Java 25; these rules apply to every block.

- **Types:** `Money` = `BigDecimal` with an explicit scale; `LocalDate` for business dates; `Instant` (UTC) for timestamps; `Sha256` = 64 hex characters.
- **Transactions:** `TX { … }` means one database transaction. Anything inside commits together or not at all, including outbox events and audit records.
- **Outbox:** `emit(topic, key, event)` inside a `TX` writes to the outbox table; the relay publishes later. Code never sends to Kafka directly.
- **Holds:** `hold(run, code, message)` moves the run to `HELD` with a reason code and plain-language message, emits an event and alerts operators. It returns STOPPED and ends the current stage, unless a person has already accepted that hold code for this run, in which case it returns CONTINUE and records the override. "return hold(...)" in the code below is shorthand for "stop here if the hold stopped the run".
- **Errors:** only expected business problems become holds or rejects. Unexpected exceptions are logged with the run ID, the stage is retried up to 3 times with backoff, then held with code `SYSTEM_ERROR`.
- **Idempotency:** every write uses a natural key with `INSERT … ON CONFLICT` or a state-checked `UPDATE`. Every function below can be called twice safely.
- **Logging:** IDs only (run, file, account key hash). Never field values.
- **Time:** `now()` returns UTC and is only used for timestamps, never inside statement content; statements use the run's pinned statement date and run date.

## Intake

A file is registered exactly once, only when it is complete, and a crash at any point leads to the same result on the next poll.

```text
function pollClientFolder(client):
    for candidate in listFiles(client.sftpFolder):
        if matches(candidate.name, client.ignorePatterns):      # .tmp, .part, hidden files
            continue
        if not isComplete(client, candidate):
            continue
        registerFile(client, candidate.stream(), candidate.name,
                     source = SFTP, sourceStamp = (candidate.path, candidate.size, candidate.mtime))

function isComplete(client, f):
    seen = stabilityCache.get(f.path)
    stabilityCache.put(f.path, (f.size, f.mtime, now()))
    stable = seen != null and seen.size == f.size and seen.mtime == f.mtime
             and now() - seen.firstSeen >= 60 seconds
    switch client.completionRule:
        case RENAME:  return stable                               # client uploads as .tmp then renames
        case MARKER:  m = findMarker(f)                           # e.g. statements.txt.done
                      if m == null: return false
                      if m.declaresSize and m.size != f.size: return false
                      return stable
        case STABLE:  return stable

function registerFile(client, stream, originalName, source, sourceStamp, uploader = null):
    if streamLength(stream) == 0:
        quarantine(client, originalName, EMPTY_FILE); return
    if rawStore.freeSpace() < expectedSize(stream) * 2 + config.diskReserve:
        alert(DISK_LOW); pauseIntake(); return                  # file stays on SFTP

    tmp = rawStore.newTempFile()
    (size, sha) = copyWhileHashing(stream, tmp)                  # SHA-256, streaming
    if source == SFTP and size != sourceStamp.size:
        delete(tmp); return                                      # changed under us; next poll

    TX {
        existing = select received_file where client = client.id and sha256 = sha
        if existing != null:
            delete(tmp)
            if existing.sourceStamp == sourceStamp:              # same file seen again after a crash
                pass                                             # not a resend: just archive below
            else:
                insert intake_event(DUPLICATE, client, originalName, existing.id)
                notifyOperators(DUPLICATE_FILE, client, originalName, existing)
        else:
            finalPath = rawStore.pathFor(client, sha)            # content-addressed
            atomicMove(tmp, finalPath, replaceIfSameHash = true)
            fileId = insert received_file(client, source, originalName, sha, size,
                                          finalPath, uploader, sourceStamp, receivedAt = now())
            if exists received_file where client = client.id and originalName = originalName
                                        and sha256 != sha:
                flag(fileId, SAME_NAME_DIFFERENT_CONTENT)
            emit("file.received.v1", key = client.id, {fileId, client, sha, size, source})
            audit(FILE_RECEIVED, fileId, actor = uploader or SYSTEM)
    }
    if source == SFTP:
        moveOnSftp(sourceStamp.path, client.processedFolder)     # only after commit
```

**Edge cases handled**

- A crash after the commit but before the SFTP move: the next poll finds the same file with the same source stamp and archives it without raising a false duplicate.
- A client resending last month's file under a new name: same hash, different source stamp, so operators are told and nothing is processed.
- Manual uploads call the same `registerFile` with `source = MANUAL` and the uploader's ID, after the upload page has checked size and type.

## Identify and pin

The run reads just enough of the file to learn its statement date, then fixes every version it will use before the full file is touched.

```text
on event "file.received.v1" (e):
    TX {
        run = insert run(fileId = e.fileId, client = e.client, type = NORMAL, state = RECEIVED)
              on conflict (fileId, type) do nothing returning existing     # event seen twice
    }
    if run.state == RECEIVED: identify(run)

function identify(run):
    file = load received_file(run.fileId)
    profiles = select mapping_profile where client = run.client and active
    matches = [p for p in profiles if p.fileNamePattern matches file.originalName]
    if matches.size == 0: return hold(run, PROFILE_NOT_FOUND, "No mapping profile matches " + file.originalName)
    if matches.size > 1: return hold(run, PROFILE_AMBIGUOUS, "More than one profile matches " + file.originalName)
    profile = matches[0]

    # identification rules live on the profile itself, not on a version,
    # so they are stable before we know which version applies
    header = peek(file, profile.identification.maxBytes)          # e.g. first 64 KB or first record
    date = extractDate(header, file.originalName, profile.identification)
    if date == null: return hold(run, MISSING_STATEMENT_DATE, "Statement date not found in header or file name")
    if date > today() + 31 days or date < today() - 400 days:
        return hold(run, STATEMENT_DATE_OUT_OF_RANGE, "Statement date " + date + " looks wrong")

    other = select run where client = run.client and profile = profile.id and statementDate = date
                       and type = NORMAL and state != CANCELLED and id != run.id
    if other != null:
        return hold(run, SECOND_FILE_SAME_PERIOD, "A file for " + date + " was already processed")

    TX { update run set state = IDENTIFIED, profileId = profile.id, statementDate = date
         where id = run.id and state = RECEIVED }
    pin(run)

function pin(run):
    d = run.statementDate
    pv  = latestApproved(mapping_profile_version, profile = run.profileId, effectiveOn = d)
    tv  = latestApproved(template_version, bindingFor(run.client, d).template, effectiveOn = d)
    ov  = bindingFor(run.client, d).presetVersion
    overrides = bindingFor(run.client, d).overrides
    if pv == null or tv == null or ov == null:
        return hold(run, NO_EFFECTIVE_VERSION, "No approved version in force on " + d)

    conds   = transitiveConditions(pv, tv, ov, overrides, effectiveOn = d)   # conditions that use conditions
    lookups = referencedLookups(pv, conds, effectiveOn = d)
    assets  = resolveSlots(tv.assetSlots, effectiveOn = d)                  # slot -> approved asset hash
    fixes   = approvedCorrections(run.client, coveringDate = d)

    problems = checkCompatibility(produced = pv.outputFields,
                                  needed = tv.fields + ov.fields(overrides) + fieldsOf(conds))
    problems += missingAssetsOrFonts(assets)
    if problems not empty:
        return hold(run, INCOMPATIBLE_VERSIONS, describe(problems))

    TX {
        insert run_pin(run, kind, versionId) for each of pv, tv, ov, conds, lookups, assets, fixes
        update run set state = PINNED, runDate = today(), overridesSnapshot = overrides
        where id = run.id and state = IDENTIFIED
        emit("run.events.v1", run.id, PINNED)
    }
```

**Edge cases handled**

- Duplicate `file.received` events create one run, because `(fileId, type)` is unique.
- A second file for the same client, profile and date is held, not silently processed; an operator decides whether it is a correction or a mistake.
- Overrides are copied onto the run, so editing a client's overrides later cannot change a pinned run.

## Mapping

One mapping routine handles every format: a format-specific reader turns the file into a stream of units, and everything after that is shared.

### Main loop

```text
function mapRun(run):
    TX {                                                         # restart always starts clean
        delete run_record, run_reject where run = run.id
        update run set state = MAPPING, mappingStartedAt = now()
        where id = run.id and state in (PINNED, MAPPING)
    }
    p = pinnedProfile(run); file = load received_file(run.fileId)
    reader = readerFor(p.format)          # DELIMITED | FIXED_WIDTH | XML | JSON
    c = Counters(units = 0, mappedUnits = 0, rejectedUnits = 0, skippedUnits = 0, records = 0)
    controls = ControlTotals(p.controls)  # e.g. count of detail records, sum of amounts
    seenAccounts = HashSet<Sha256>()      # about 40 MB for a million accounts
    currentAccount = null                 # flat files: the account the next detail lines belong to
    trailer = null; batch = []

    for unit in reader.units(file, p):    # streaming; never the whole file in memory
        c.units += 1
        if unit.isError:                  # structural problem the reader found
            reject(run, unit.position, unit.errorCode, unit.message); c.rejectedUnits += 1
        else if unit.recordType in p.skipTypes:
            c.skippedUnits += 1
            if unit.recordType == p.trailerType:
                if trailer != null: return hold(run, TWO_TRAILERS, "The file has more than one trailer")
                trailer = unit
        else if trailer != null:
            return hold(run, DATA_AFTER_TRAILER, "Lines found after the trailer at " + unit.position)
        else:
            result = mapUnit(unit, p, run)                # all records of the unit, or one rejection
            if result.rejected:
                reject(run, unit.position, result.code, result.message); c.rejectedUnits += 1
            else if p.isFlat and result.isLineItemsOnly and currentAccount == null:
                reject(run, unit.position, LINE_BEFORE_ACCOUNT, "Transaction line before any account line")
                c.rejectedUnits += 1
            else:
                if result.accountHeader != null:
                    k = sha256(result.accountHeader.accountKey)
                    if k in seenAccounts and p.duplicateAccountRule == HOLD:
                        return hold(run, DUPLICATE_ACCOUNT, "Account appears twice, again at " + unit.position)
                    seenAccounts.add(k); currentAccount = result.accountHeader.accountKey
                for rec in result.records:
                    if rec.accountKey == null: rec.accountKey = currentAccount   # flat detail lines
                    batch.add(rec); controls.add(rec); c.records += 1
                c.mappedUnits += 1
        if c.rejectedUnits > p.rejectTolerance:
            return hold(run, TOO_MANY_REJECTS, c.rejectedUnits + " lines could not be read")  # stop early
        if batch.size >= 10_000:
            TX { copyInsert(run_record, batch) }; batch.clear(); heartbeat(run)
    TX { copyInsert(run_record, batch) }
    reconcileMapping(run, p, c, controls, trailer, seenAccounts.size)
```

### Readers

```text
reader DELIMITED:
    decoder = strictDecoder(p.encoding)                 # malformed bytes -> error unit with line number
    skip BOM if present
    parse with RFC 4180 rules: p.delimiter, p.quote, p.escape;
        delimiters and line breaks inside quotes stay in the field
    if p.hasHeaderRow:
        if normalise(headerNames) != normalise(p.expectedHeader): fail run with LAYOUT_CHANGED
    for each logical row:
        type = p.recordTypeOf(row)                       # by a column value, or one type only
        layout = p.layouts[type] else -> error unit UNKNOWN_RECORD_TYPE
        if row.fieldCount != layout.fieldCount -> error unit WRONG_FIELD_COUNT
        yield unit(position = physical line, type, fields by column)

reader FIXED_WIDTH:
    for each line (CRLF, LF and CR all end a line):
        if line is blank and p.allowBlankLines: yield skipped unit; continue
        type = cut(line, p.typeStart, p.typeLength, p.unit)            # unit = CHARACTERS or BYTES
        layout = p.layouts[type] else -> error unit UNKNOWN_RECORD_TYPE
        len = length(line, p.unit)
        if len != layout.length and not (p.allowShortTrailingSpaces and len < layout.length):
            -> error unit WRONG_LINE_LENGTH
        yield unit(position = line number, type, fields = cut each by start and width in p.unit)

reader XML:
    StAX parser with DTDs and external entities disabled, depth limit 64
    the profile names an account element path, e.g. /Statement/Account
    for each account element: buffer that subtree only (limit p.maxAccountBytes, default 50 MB;
        over the limit -> error unit ACCOUNT_TOO_LARGE)
        yield unit(position = account index and line, type = ACCOUNT, subtree)
    a document-level trailer path, if set, becomes the trailer unit

reader JSON:
    Jackson streaming parser with nesting and string-length limits
    account array path, e.g. $.accounts[*]: buffer one account object at a time (same size limit)
    yield unit(position = array index, type = ACCOUNT, object)
```

For XML and JSON, `mapUnit` turns one account into one account-header record plus one record per repeating line item (the profile's line-item path, for example `Transactions/Transaction`). Values from the account level are copied onto each line-item record.

### Mapping one unit

```text
function mapUnit(unit, p, run):              # returns all records of the unit, or one rejection
    result = UnitResult()
    for (recordType, sourceNode) in expand(unit, p):            # flat files: just the unit itself
        rec = Record(type = recordType, position = sourceNode.position)
        for m in p.fieldMappings[recordType]:
            raw = extract(sourceNode, m.source)                 # column, offset, or element/attribute path
            v = clean(raw, m.cleanups)                          # trim, case, pad removal, lookups
            v = parseAs(v, m.type, m.format)                    # see rules below
            if v.failed: return Rejected(BAD_VALUE, m.label + ": " + v.reason)
            if m.required and v.isEmpty: return Rejected(MISSING_VALUE, m.label + " is empty")
            rec[m.target] = v
        for rule in p.conditionalRules[recordType]:            # in the order shown in the studio
            if evaluate(rule.when, rec, pinnedLookups(run)): rec[rule.target] = evaluate(rule.value, rec)
        if recordType == p.accountHeaderType:
            rec.accountKey = normaliseKey(rec[p.accountKeyField])   # trim, upper case; leading zeros only if the profile says
            if rec.accountKey is empty: return Rejected(MISSING_ACCOUNT_KEY, "Account number is empty")
            applyCorrections(rec, pinnedCorrections(run))           # audited one-off fixes, by account key
            result.accountHeader = rec
        else if p.isNested:
            rec.accountKey = result.accountHeader.accountKey        # XML/JSON: copied from the account level
        result.records.add(rec)
    result.isLineItemsOnly = (result.accountHeader == null)
    return result
```

**Value parsing rules**

- **Numbers:** decimal and thousands separators come from the profile, never the server locale. Negative styles are declared: `-12.00`, `12.00-`, `(12.00)` or a CR/DR marker in a named field. Implied decimals turn `000012345` with 2 implied places into `123.45`. More digits than the field's precision is a reject, never a silent rounding.
- **Dates:** parsed with the declared pattern in strict mode, so `31/02/2026` is a reject.
- **Text:** control characters other than tab are removed; length over the field's maximum is a reject or a cut, as the profile says.
- **Lookups:** a code missing from the lookup table is a reject unless the profile names a default.

### Reconciliation

```text
function reconcileMapping(run, p, c, controls, trailer, accountCount):
    if c.units != c.mappedUnits + c.rejectedUnits + c.skippedUnits:   # every unit accounted for
        return hold(run, COUNT_MISMATCH, "Some lines were not accounted for")
    if p.requiresTrailer and trailer == null:
        return hold(run, MISSING_TRAILER, "No trailer line: the file may be cut short")
    if trailer != null:
        for check in p.controls:                              # record count, sum of amounts
            expected = parseAs(extract(trailer, check.source), check.type, check.format)
            if expected != controls.value(check):            # exact BigDecimal compare
                return hold(run, CONTROL_TOTAL_MISMATCH,
                            check.label + ": file says " + expected + ", records add up to " + controls.value(check))
    if p.balanceCheck.enabled:
        bad = checkAccountBalances(run, p.balanceCheck)
        if bad.count > 0:
            return hold(run, BALANCE_MISMATCH, bad.count + " accounts do not balance, for example " + bad.examples)
    if accountCount == 0:
        return hold(run, NO_ACCOUNTS, "The file contains no accounts")
    TX {
        update run set state = MAPPED, units = c.units, records = c.records, rejects = c.rejectedUnits,
                       skipped = c.skippedUnits, accounts = accountCount, controlTotals = controls.snapshot()
        where id = run.id and state = MAPPING
        emit("run.events.v1", run.id, MAPPED)
    }
    startRecordsExport(run)    # publishes run_record to records.clean.v1 in the background, keyed by run + account
```

### Per-account balance check

Every account must satisfy opening balance + transactions = closing balance, to the cent, before rendering starts; this catches a wrong statement that every file-level total would miss.

```text
function checkAccountBalances(run, bc):        # bc names the opening, closing and amount fields
    TX {
        delete run_balance_issue where run = run.id                 # safe to repeat
        insert into run_balance_issue (run, accountKey, opening, movement, closing, difference)
        select h.run, h.accountKey,
               h.data[bc.openingField], coalesce(sum(d.data[bc.amountField]), 0), h.data[bc.closingField],
               h.data[bc.openingField] + coalesce(sum(d.data[bc.amountField]), 0) - h.data[bc.closingField]
        from run_record h
        left join run_record d on d.run = h.run and d.accountKey = h.accountKey
                              and d.type in bc.movementTypes        # detail lines that move the balance
        where h.run = run.id and h.type = bc.accountHeaderType
        group by h.run, h.accountKey, h.data[bc.openingField], h.data[bc.closingField]
        having h.data[bc.openingField] + coalesce(sum(d.data[bc.amountField]), 0) - h.data[bc.closingField] != 0
    }
    issues = select run_balance_issue where run = run.id order by abs(difference) desc
    return (count = issues.size, examples = first 5 account keys masked to the last 4 characters)
```

- All amounts are exact decimals in the database (`numeric`), so the comparison is exact; there is no tolerance unless the profile sets one, for example 0.01 for a client known to round each line.
- An account with no detail lines is checked too: its opening and closing balances must match.
- The full list of failing accounts is shown in the studio and on the control report; the hold message carries only five masked examples, because hold messages go out in alerts.
- Clients whose files carry no balances have the check switched off in their profile, and the control report says so.

**Edge cases handled**

- A file cut short in transfer has no trailer or wrong totals and is held, never half-processed.
- Records are exported to Kafka by a separate background job reading `run_record`, rather than through the outbox, so a million records do not flood the outbox table; the export is keyed and safe to repeat.

## Rule evaluation

The rule builder saves rules as a typed tree that the platform walks itself, so a rule can never run code, loop forever or fail differently on different data types.

```text
Condition := All(conditions) | Any(conditions) | Not(condition)
           | Compare(field, op, operand)          # op: = != < <= > >=
           | IsEmpty(field) | InList(field, values) | Matches(field, pattern)
           | Named(conditionId)                    # resolved to the pinned version
Value     := Literal(typed) | Field(name) | Lookup(table, keyValue)
           | Concat(values) | Arith(op, left, right, scale, rounding)

function evaluate(node, rec, ctx, depth = 0):
    if depth > 20: throw RuleError("Rule nested too deeply")
    switch node:
        All(cs):  return every(evaluate(c, rec, ctx, depth + 1) for c in cs)     # short-circuits
        Any(cs):  return some(evaluate(c, rec, ctx, depth + 1) for c in cs)
        Not(c):   return not evaluate(c, rec, ctx, depth + 1)
        IsEmpty(f): v = rec[f]; return v == null or (v is Text and trim(v) == "")
        Compare(f, op, x):
            a = rec[f]; b = valueOf(x, rec, ctx)
            if a == null or b == null: return false        # missing never throws; use IsEmpty to test it
            if a is Money: return compareOp(a.compareTo(b), op)   # compareTo, so 1.0 equals 1.00
            if a is Text:  return compareOp(collate(nfc(a), nfc(b), node.caseSensitive), op)
            if a is Date:  return compareOp(a.compareTo(b), op)
        InList(f, vs): return rec[f] != null and any(equalsTyped(rec[f], v) for v in vs)
        Matches(f, pat): return rec[f] != null and re2(pat).matches(rec[f])   # linear-time engine, no ReDoS
        Named(id): return evaluate(ctx.pinnedCondition(id).root, rec, ctx, depth + 1)

function valueOf(v, rec, ctx):
    Literal(x): return x
    Field(f): return rec[f]
    Lookup(t, k): return ctx.pinnedLookup(t).get(valueOf(k, rec, ctx))    # missing -> null
    Concat(vs): return join(text(valueOf(x, rec, ctx)) for x in vs if not null)
    Arith(op, l, r, scale, rounding):
        a = valueOf(l, rec, ctx); b = valueOf(r, rec, ctx)
        if a == null or b == null: return null
        if op == DIVIDE and b.signum() == 0: throw RuleError("Division by zero")
        return apply(op, a, b).setScale(scale, rounding)      # scale and rounding are part of the rule
```

**Checked when a rule is saved, not when it runs**

- Every field exists in the profile or template and has the type the operator needs; a text field cannot be compared to an amount.
- Named conditions form no cycle (A uses B, B uses A); the studio refuses a rule that would create one.
- Patterns compile in the linear-time engine (RE2J) and are at most 200 characters.
- Each rule is shown with a plain-language sentence generated from its tree, so the user can read back what it will do.

A `RuleError` while mapping rejects that line; while rendering it fails that account; either way the run is held with the rule's name in the message.

## Rendering

Filters run before rendering so excluded accounts are never drawn; every kept account then gets exactly one verified PDF, handed out through a leased work queue that survives worker crashes.

### Preparing statements

```text
function prepareStatements(run):
    TX { update run set state = RENDERING where id = run.id and state = MAPPED }
    onRunEntersRendering(run)                                    # joins the fair-share queue
    ov = pinnedPreset(run).withOverrides(run.overridesSnapshot)
    for acct in streamAccounts(run, orderBy = accountKey):        # header + its lines, from run_record
        keep = true; reason = null
        for f in ov.filters:                                      # e.g. exclude zero balance, closed, test
            if evaluate(f.when, acct, ctx(run)): keep = false; reason = f.name; break
        channels = [ch.name for ch in ov.channels if evaluate(ch.when, acct, ctx(run))]   # print, email, archive
        if keep and channels is empty:
            keep = false; reason = "No delivery channel applies"   # never silently dropped: counted and reported
        insert statement(run, acct.key, status = keep ? PENDING : FILTERED, filterReason = reason,
                         channels, lineCount = acct.lines.size,
                         sizeClass = acct.lines.size > ov.largeAccountLines ? LARGE : NORMAL)
            on conflict (run, accountKey) do nothing
```

### Worker loop

```text
# Fair scheduling: each active run has a virtual clock. A run's clock advances by
# (accounts claimed / its priority weight), and workers always serve the run whose clock is lowest.
# A small client therefore gets its turn even while a million-account run is in progress,
# and a run marked urgent (weight 4) moves four times as fast.

function onRunEntersRendering(run):
    TX { update run set virtualTime = coalesce((select min(virtualTime) from run where state = RENDERING), 0)
         where id = run.id }          # a new run joins at the current minimum, never far behind or ahead

function workerLoop(me, sizeClass):
    while running:
        items = []
        candidates = select id, priorityWeight from run
                     where state = RENDERING and hasClaimableWork(id, sizeClass)
                     order by virtualTime, id                     # no lock: just the order to try
        for r in candidates:
            items = TX {
                claimed = select statement
                          where run = r.id and sizeClass = sizeClass
                            and (status = PENDING or (status = IN_PROGRESS and leaseUntil < now()))
                          order by id
                          for update skip locked limit 50
                          -> update set status = IN_PROGRESS, worker = me,
                                        leaseUntil = now() + 10 minutes, attempts = attempts + 1
                if claimed not empty:
                    update run set virtualTime = virtualTime + claimed.size / r.priorityWeight
                    where id = r.id and state = RENDERING          # short row update; never held while rendering
                return claimed
            }
            if items not empty: break                               # otherwise try the next run in line
        if items is empty: waitForWork(); continue
        for st in items:
            if runState(st.run) != RENDERING: releaseLease(st); continue    # run held while we worked
            renderOne(me, st)

function renderOne(me, st):
    if st.attempts > 3:
        return failStatement(st, TOO_MANY_ATTEMPTS)
    ctx = runContextCache.get(st.run)       # template, CSS, fonts, assets, conditions: loaded once per run per worker
    acct = loadAccount(st.run, st.accountKey)                     # lines in the template's declared order
    try:
        html = ctx.template.render(acct, statementDate = ctx.run.statementDate, runDate = ctx.run.runDate)
               # auto-escaping on: a customer name containing < or & can never break or inject markup
        listener = LayoutListener()          # collects text overflow events
        pdf = renderer.render(html,
                              fonts = ctx.fontLibrary,             # only library fonts; no system fonts
                              resources = ctx.assetStore,          # only pinned assets; no network access
                              timeout = ctx.limits.perAccount,
                              listener)
        if not allFontsEmbeddedAndKnown(pdf, ctx.fontLibrary):
            throw RenderError(FONT_PROBLEM, "A font was missing or substituted")
        for e in listener.overflows:
            if ctx.template.overflowRule(e.box) == FAIL: throw RenderError(TEXT_OVERFLOW, e.box + " is too long")
        pages = pdf.pageCount
        sheets = ctx.duplex ? ceil(pages / 2) : pages
        (path, sha) = safeWrite(shardedPath(st.run, st.accountKey), pdf.bytes)     # temp, verify, rename
        TX {
            update statement set status = RENDERED, pages, sheets, path, sha256 = sha,
                                 overflowWarnings = listener.overflows
            where id = st.id and status = IN_PROGRESS and worker = me     # lease still ours
        }
    catch RenderError e:              # deterministic: retrying gives the same answer
        failStatement(st, e.code, e.message)
    catch other:                      # system problem: let another attempt try, up to 3
        log(st.id, other); releaseLease(st)
```

The run row is only updated briefly after a claim and never locked while rendering, so many workers can still share one large run when it is the only one active. Lines inside a statement are loaded in the template's declared order (default: date, then source position), so two runs never list same-day transactions differently.

### Finishing the stage

```text
function checkRenderingDone(run):
    open = count statement where run = run.id and status in (PENDING, IN_PROGRESS)
    if open > 0: return
    failed = count statement where run = run.id and status = FAILED
    if failed > 0: return hold(run, RENDER_FAILURES, failed + " statements could not be produced")
    kept = count statement where run = run.id and status != FILTERED
    rendered = count statement where run = run.id and status = RENDERED
    if kept != rendered: return hold(run, COUNT_MISMATCH, "Rendered count does not match accounts kept")
    for st in sample(statements where run = run.id and status = RENDERED, n = 5):
        verifyFile(st.path, st.sha256)                            # spot-check files on disk
    TX { update run set state = AWAITING_RELEASE or OUTPUT (per client setting)
         where id = run.id and state = RENDERING }
```

**Edge cases handled**

- A worker that dies mid-account loses its lease after 10 minutes and another worker redoes the account; the state check on update stops the late worker from overwriting.
- A customer name with HTML characters is escaped, not interpreted.
- One enormous account is rendered by the large-account pool, so it cannot stall thousands of normal ones.

## Output

The whole output is planned and checked before a single file is written, so name clashes, missing folders and full disks are found while nothing has been delivered.

### Planning

```text
function runOutput(run):
    if run.state != OUTPUT: return            # set by checkRenderingDone, or by an operator's release
    ov = pinnedPreset(run).withOverrides(run.overridesSnapshot)
    plan = buildPlan(run, ov)
    problems = validatePlan(plan, ov)
    if problems not empty: return hold(run, OUTPUT_PLAN_PROBLEM, describe(problems))
    TX { insert output_file(run, path, channel, seq, statementIds, expectedPages, status = PLANNED)
         for f in plan.files on conflict (run, path) do nothing }
    for f in outputFiles(run) where status != WRITTEN order by seq:
        assemble(run, f, ov)
    reconcileOutput(run)

function buildPlan(run, ov):
    files = []
    for ch in ov.channels:                                      # print, email, archive
        sts = statements where run = run.id and status = RENDERED and ch.name in channels
        sorted = sortBy(sts, ov.sortKeys(ch) + [accountKey, firstSourcePosition])   # ties always broken the same way
        for group in consecutiveGroups(sorted, ov.groupFields(ch)):
            chunks = splitBetweenAccounts(group, maxAccounts = ov.maxAccounts,
                                          maxSheets = ov.maxSheets, maxBytes = ov.maxBytes)
                     # never splits an account; an account over a limit gets a file of its own
            for (i, chunk) in enumerate(chunks):
                tokens = {client, yyyy, mm, dd from run.statementDate, channel = ch.name,
                          group values, part = i + 1, parts = chunks.size, run = shortId(run)}
                files.add(PlannedFile(path = buildPath(ov.root(ch), ov.pathTemplate(ch), tokens),
                                      channel = ch.name, statements = chunk, pages = sum(pagesWithBlanks(chunk))))
    return files
```

Sort values are typed: amounts and dates compare as amounts and dates, text uses one fixed collation for every server, so `10` never sorts before `9` and a server's language settings cannot change the order.

```text
function validatePlan(plan, ov):
    problems = []
    seen = {}
    for f in plan.files:
        key = caseFold(normaliseUnicode(f.path))              # Report.pdf and report.pdf are the same file
        if key in seen: problems.add("Two files would be named " + f.path)
        seen[key] = true
        if length(f.path) > ov.maxPathLength: problems.add("Path too long: " + f.path)
        if not isInside(f.path, ov.root(f.channel)): problems.add("Path leaves its root folder: " + f.path)
        if exists(f.path) and not writtenByThisRun(f.path, run) and ov.existingFileRule == STOP:
            problems.add("File already exists: " + f.path)       # our own file from before a crash is not a clash
    for folder in distinct(parent(f.path) for f in plan.files):
        if not createDirectories(folder) or not canWriteTestFile(folder):
            problems.add("Cannot create or write to " + folder)
    if freeSpace(each root) < sum(sizes of its files) * 1.2:
        problems.add("Not enough disk space in " + root)
    return problems
```

The preset studio refuses a preset whose group fields are not the first sort keys, because grouping on a field the list is not sorted by would scatter one branch across many files.

### Assembling a file

```text
function assemble(run, f, ov):
    if f.status == RENAMING and exists(f.finalPath) and sha256(f.finalPath) == f.sha256:
        return markWritten(f)                                  # crash happened after the rename: already done
    tmp = tempFileNextTo(f.path)                              # same folder, so the final rename is atomic
    doc = PdfMerger(memory = TEMP_FILES)
    index = []; page = 1
    for st in f.statements:                                   # already in final order
        if sha256(st.path) != st.sha256: return hold(run, FILE_CORRUPTED, "Statement file changed on disk: " + st.id)
        doc.append(st.path)
        addedBlank = ov.duplex and isOdd(st.pages)
        if addedBlank: doc.appendBlankPage(sizeOfLastPage)    # next statement starts on a new sheet
        index.add(account = st.accountKey, file = f.path, startPage = page, endPage = page + st.pages - 1)
        page += st.pages + (addedBlank ? 1 : 0)
    if f.channel == PRINT and ov.omr.enabled:
        stampOmr(doc, f, ov.omr, run)                         # after the final order is known
    if ov.pdfA: doc = toPdfA(doc); if not validPdfA(doc): return hold(run, PDFA_FAILED, f.path)
    doc.save(tmp)
    if pageCount(tmp) != f.expectedPages: delete(tmp); return hold(run, PAGE_COUNT_MISMATCH, f.path)
    sha = sha256(tmp)
    finalPath = chooseFinalPath(f.path, ov.existingFileRule)  # overwrite, add _v2, or stop (checked in the plan)
    TX { update output_file set status = RENAMING, sha256 = sha, finalPath = finalPath where id = f.id }
    renameWithRetry(tmp, finalPath, timeout = 60 seconds)     # Windows: retries while another program holds the file
    writeIndexFile(f, index)                                  # CSV, same temp-then-rename method
    markWritten(f)                                            # status = WRITTEN, pages = page - 1
```

### OMR marks

```text
function stampOmr(doc, f, spec, run):
    sheetSeq = 0
    for st in f.statements:
        sheets = st.sheets
        if sheets > spec.maxSheetsPerEnvelope:
            return hold(run, ENVELOPE_TOO_FULL, st.accountKey)   # routing rule should have sent it elsewhere
        inserts = [i for i in spec.inserts if evaluate(i.when, st.account, ctx(run))]   # e.g. add flyer B
        for s in 1..sheets:
            sheetSeq += 1
            marks = [BENCHMARK]
            if s == sheets: marks += [END_OF_COLLATION] + inserts.marks
            marks += sequenceMarks(sheetSeq mod spec.sequenceModulo)      # wrap-around sequence check
            marks += parityMark(marks, spec.parity)                       # makes the total even or odd
            drawMarks(doc.frontPageOfSheet(st, s), marks, spec.position, spec.lineThickness, spec.spacing)
```

Templates reserve the OMR clear zone; the template studio refuses any element placed inside it, so marks are never printed over content.

### Reconciling output

```text
function reconcileOutput(run):
    for ch in channels(run):
        inFiles = statementIdsInWrittenFiles(run, ch)
        expected = statementIds(run, status = RENDERED, channel = ch)
        if sorted(inFiles) != sorted(expected):                # catches missing and duplicated statements
            return hold(run, OUTPUT_MISMATCH, ch + ": statements in files do not match statements rendered")
    if any output_file where run = run.id and status != WRITTEN: return
    writeControlReport(run)                                    # the full reconciliation chain, saved with the output
    TX { update run set state = DELIVERING where id = run.id and state = OUTPUT }
```

## Delivery

Each file goes to each of its destinations exactly once in effect: attempts are recorded, retried with growing waits, and verified wherever the destination allows.

```text
function deliverRun(run):
    if run.state != DELIVERING: return
    if run.type == TEST: destinationsMustBeTest(run)            # a test run can never reach production
    for f in outputFiles(run, status = WRITTEN):
        for dest in destinationsFor(f.channel, pinnedPreset(run)):
            TX { insert delivery(outputFile = f.id, destination = dest.id, status = PENDING)
                 on conflict (outputFile, destination) do nothing }
    for d in deliveries(run) where status in (PENDING, RETRY_WAIT) and nextAttemptAt <= now():
        deliverOne(run, d)
    if any delivery where run = run.id and status = FAILED:
        return hold(run, DELIVERY_FAILED, describeFailures(run))
    if all deliveries where run = run.id have status = DELIVERED:
        TX {
            update run set state = COMPLETE, completedAt = now() where id = run.id and state = DELIVERING
            emit("output.delivered.v1", run.id, summary(run))
        }

function deliverOne(run, d):
    f = outputFile(d.outputFile); dest = destination(d.destination)
    try:
        switch dest.kind:
            FOLDER, NETWORK_SHARE:
                target = dest.path(f)                           # same token rules and checks as the plan
                copyToTempThenRename(f.finalPath, target)
                ok = sha256(target) == f.sha256
            SFTP:
                upload(f.finalPath, dest, remoteName = name(f) + ".part")
                ok = remoteSize(dest, name(f) + ".part") == f.size
                if ok: remoteRename(dest, name(f) + ".part", name(f))   # receiver never sees a partial file
            ARCHIVE:
                ok = archiveClient.store(f, index = indexFile(f), idempotencyKey = f.sha256)
        if not ok: throw DeliveryError("Check after copy failed")
        TX { update delivery set status = DELIVERED, deliveredAt = now(), attempts = attempts + 1 where id = d.id }
        audit(DELIVERED, f.id, dest.id)
    catch error:
        n = d.attempts + 1
        if n >= 5:
            TX { update delivery set status = FAILED, attempts = n, lastError = safeMessage(error) where id = d.id }
        else:
            TX { update delivery set status = RETRY_WAIT, attempts = n,
                                    nextAttemptAt = now() + (2 ^ n) minutes, lastError = safeMessage(error)
                 where id = d.id }
```

The archive call carries the file's SHA-256 as an idempotency key, so a retry after a timeout cannot store the same file twice.

## Orchestrator

One orchestrator at a time drives every run from state to state; each step is looked up from the run's current state, so a restart simply continues where the database says the run is.

### Main loop

```text
STEPS = {
    RECEIVED: identify,      IDENTIFIED: pin,           PINNED: mapRun,
    MAPPING: mapRun,         MAPPED: prepareStatements, RENDERING: checkRenderingDone,
    OUTPUT: runOutput,       DELIVERING: deliverRun
}   # AWAITING_RELEASE and HELD wait for a person; COMPLETE and CANCELLED are final

function orchestratorMain():
    while running:
        if not holdsAdvisoryLock(ORCHESTRATOR): tryAcquire(); sleep(5 seconds); continue   # standby node waits
        for run in select run where state in keys(STEPS) and (nextStepAt <= now() or nextStepAt is null)
                   for update skip locked limit 20:
            try:
                STEPS[run.state](run)
                heartbeat(run)
            catch unexpected e:
                n = incrementStepFailures(run)
                if n >= 3: hold(run, SYSTEM_ERROR, "Unexpected error in " + run.state + "; see log " + logRef(e))
                else: setNextStepAt(run, now() + (2 ^ n) minutes)
        for run in select run where state in (MAPPING, RENDERING, OUTPUT, DELIVERING)
                                  and heartbeatAt < now() - 10 minutes:
            alertOnce(run, STALLED)
        waitForNotifyOrTimeout(5 seconds)        # PostgreSQL LISTEN/NOTIFY wakes it early
```

### Holding and releasing

```text
function hold(run, code, message):         # returns STOPPED, or CONTINUE if a person already accepted this code
    if code in run.acceptedChecks:
        audit(OVERRIDE_APPLIED, run.id, code, message)      # recorded, and printed on the control report
        return CONTINUE
    TX {
        updated = update run set state = HELD, heldFromState = state, holdCode = code,
                                 holdMessage = message, heldAt = now()
                  where id = run.id and state not in (HELD, COMPLETE, CANCELLED)
        if updated == 1:
            emit("run.events.v1", run.id, HELD(code, message)); audit(RUN_HELD, run.id, code)
    }
    if updated == 1: notifyOperators(run, code, message)
    return STOPPED

RELEASE_ACTION = {                       # what a release does for each kind of hold
    SECOND_FILE_SAME_PERIOD: ACCEPT_AND_RESUME, TOO_MANY_REJECTS: ACCEPT_AND_RESUME,
    CONTROL_TOTAL_MISMATCH: ACCEPT_AND_RESUME,  DUPLICATE_ACCOUNT: ACCEPT_AND_RESUME,
    BALANCE_MISMATCH: ACCEPT_AND_RESUME,
    INCOMPATIBLE_VERSIONS: REPIN,        NO_EFFECTIVE_VERSION: REPIN,  PROFILE_NOT_FOUND: REIDENTIFY,
    RENDER_FAILURES: RETRY_FAILED,       DELIVERY_FAILED: RETRY_FAILED,
    OUTPUT_PLAN_PROBLEM: RESUME,         STALLED: RESUME,              SYSTEM_ERROR: RESUME,
    FILE_CORRUPTED: RESTART_RENDERING,   MISSING_TRAILER: NOT_RELEASABLE
}

function release(run, user, note):
    requireReauthenticated(user)
    action = RELEASE_ACTION[run.holdCode] or RESUME
    requireRole(user, run.client, action == ACCEPT_AND_RESUME ? APPROVER : OPERATOR)
    if run.client.twoPersonRelease and user.id == run.startedBy: refuse("Another person must release this run")
    if action == NOT_RELEASABLE: refuse("This run can only be cancelled; ask the client for a new file")
    if action == ACCEPT_AND_RESUME and note is empty: refuse("Give a reason for accepting")
    target = switch action:
        RESUME, ACCEPT_AND_RESUME: run.heldFromState
        REIDENTIFY:        RECEIVED
        REPIN:             IDENTIFIED      # new pins mean records must be mapped again
        RETRY_FAILED:      run.heldFromState  (and reset FAILED statements or deliveries to PENDING, attempts to 0)
        RESTART_RENDERING: MAPPED          (and delete the run's statements and files)
    TX {
        update run set state = target, holdCode = null, releasedBy = user.id, releaseNote = note,
                       acceptedChecks = acceptedChecks + (action == ACCEPT_AND_RESUME ? [run.holdCode] : []),
                       nextStepAt = now()
        where id = run.id and state = HELD
        audit(RUN_RELEASED, run.id, user.id, action, note)
    }
```

An accepted check is stored on the run, so when the stage runs again the same check lets it through instead of holding it a second time; each acceptance is printed on the control report. A duplicate file has no run of its own, so forcing it through is a separate "reload" action that creates a run of type \`RELOAD\` for the existing file, with a required reason.

### Release before output

```text
function proofSet(run):                    # what the operator reviews in Awaiting release
    picks = first and last statement in each channel
          + the 3 longest statements
          + every statement with an overflow warning (up to 50)
          + 10 statements chosen with a random generator seeded by the run ID   # same picks every time
    return distinct(picks)

function releaseForOutput(run, user):
    requireRole(user, run.client, OPERATOR); requireReauthenticated(user)
    TX { update run set state = OUTPUT, releasedBy = user.id where id = run.id and state = AWAITING_RELEASE
         audit(RUN_RELEASED_FOR_OUTPUT, run.id, user.id) }
```

### Cancelling and reprinting

```text
function cancel(run, user, reason):
    delivered = count delivery where run = run.id and status = DELIVERED
    if delivered > 0 and not confirmedByUser("Some files were already delivered: " + delivered):
        return
    TX { update run set state = CANCELLED, cancelledBy = user.id, cancelReason = reason
         where id = run.id and state not in (COMPLETE, CANCELLED)
         audit(RUN_CANCELLED, run.id, user.id, reason) }      # records and files are kept for audit

function reprint(original, user, accountKeys = all, withCurrentVersions = false):
    r = insert run(fileId = original.fileId, client, type = REPRINT, parentRun = original.id,
                   statementDate = original.statementDate, accountFilter = accountKeys, startedBy = user.id)
    if withCurrentVersions: r.state = IDENTIFIED                     # pins fresh versions
    else: copyPins(original, r); r.state = PINNED                     # identical to the original
    # mapping repeats from the raw file; the account filter applies in prepareStatements;
    # output paths include the run's short ID, so a reprint never overwrites the original's files
```

## Helpers: paths and safe writing

Every generated folder or file name goes through one function that cleans each value and proves the result stays inside its root, and every file goes through one write routine that never leaves a partial file visible.

```text
WINDOWS_RESERVED = {CON, PRN, AUX, NUL, COM1..COM9, LPT1..LPT9}

function buildPath(root, pathTemplate, tokens):          # e.g. "{client}/{yyyy}/{mm}/{branch}/{client}_{branch}_part{part}.pdf"
    parts = []
    for segment in split(pathTemplate, "/"):             # templates always use /, on every platform
        s = replaceTokens(segment, tokens, clean = cleanSegment)   # unknown token -> error at preset save
        if s is empty: throw PathError("A folder or file name came out empty in " + segment)
        parts.add(s)
    p = root.resolve(join(parts)).normalize()
    if not p.startsWith(root.normalize()): throw PathError("Path leaves its root folder")
    return p

function cleanSegment(value):
    v = normaliseUnicode(NFC, text(value))
    v = remove characters in  / \ : * ? " < > |  and control characters
    v = replace ".." with "_"
    v = trim spaces and dots from both ends               # Windows drops trailing dots and spaces silently
    if upper(baseName(v)) in WINDOWS_RESERVED: v = "_" + v
    return cutToBytes(v, 120)                             # keeps whole characters

function safeWrite(finalPath, bytesOrStream):
    createDirectories(parent(finalPath))
    tmp = parent(finalPath) / ("." + fileName(finalPath) + "." + uuid() + ".tmp")   # same folder: atomic rename
    write(tmp, bytesOrStream); fsync(tmp)
    sha = sha256(tmp)
    renameWithRetry(tmp, finalPath, atomic = true, timeout = 60 seconds)   # retries on Windows file locks
    fsync(parent(finalPath))                              # where the operating system supports it
    return (finalPath, sha)

function cleanupTempFiles():                              # run at start-up and daily
    delete files matching ".*.tmp" older than 24 hours under every managed root
```

The `{part}` token can be padded (`{part:3}` gives `001`) so files sort correctly in any file browser, and every path template is checked when the preset is saved, not when a run is already waiting.

## Sign-in, MFA and lockout

Sign-in takes the same time and gives the same message whether the username exists or not, and every account needs a password and an authenticator code.

```text
DUMMY_HASH = argon2id("not-a-real-password")             # computed once at start-up

function signIn(username, password, ip):
    rateLimit(ip, max = 20 per minute)                    # slows password spraying across accounts
    u = select user_account where lower(username) = lower(trim(username))
    hash = (u != null) ? u.passwordHash : DUMMY_HASH
    ok = argon2Verify(hash, password)                     # always runs, so timing reveals nothing
    if u == null or u.status != ACTIVE: return fail("Username or password is wrong")
    if u.lockedUntil > now(): return fail("Username or password is wrong")   # same message; lock is logged
    if not ok:
        TX { u.failedAttempts += 1
             if u.failedAttempts % 5 == 0:
                 u.lockedUntil = now() + 15 minutes * 2 ^ (u.failedAttempts / 5 - 1)   # 15, 30, 60 minutes...
                 notifyAdmins(ACCOUNT_LOCKED, u.id)
             audit(SIGN_IN_FAILED, u.id, ip) }
        return fail("Username or password is wrong")
    return pendingMfa(u.id, expiresIn = 5 minutes)        # no session yet

function verifyMfa(pending, code):
    u = load(pending.userId)
    if pending.expired or pending.attempts >= 5: return fail("Please sign in again")
    pending.attempts += 1
    valid = totpValid(decrypt(u.mfaSecret), code, window = 1 step)
            and code != u.lastUsedTotp                    # a code works once only
            or recoveryCodeValid(u, code)                 # stored hashed; deleted when used
    if not valid: return fail("That code is wrong or expired")
    TX { u.failedAttempts = 0; u.lastUsedTotp = code; u.lastSignIn = now()
         audit(SIGNED_IN, u.id, ip) }
    rotateSessionId()                                     # prevents session fixation
    return newSession(u.id, idle = 30 minutes, absolute = 12 hours)

function requireReauthenticated(user):                    # approvals, releases, user admin
    if user.session.lastPasswordCheck < now() - 5 minutes: ask for password and MFA code again

function setPassword(u, newPassword):
    if length(newPassword) < 12: refuse("Use at least 12 characters")
    if inBreachedList(newPassword) or contains(newPassword, u.username): refuse("Choose a less common password")
    TX { u.passwordHash = argon2id(newPassword); invalidateOtherSessions(u) ; audit(PASSWORD_CHANGED, u.id) }
```

Admins can reset a user's password and MFA, but never see or set the password themselves: a reset sends a one-time link valid for 30 minutes, and the user must set up MFA again on first sign-in.

## Retention purge

A daily job deletes data past each client's retention period, never touches anything under legal hold or still in use, and records exactly what it removed.

```text
function dailyPurge(today):
    if not holdsAdvisoryLock(PURGE): return              # only one purge at a time
    for client in clients:
        r = client.retention                               # periods set per client, per data kind
        if client.legalHold: continue
        for run in select run where client = client.id and state in (COMPLETE, CANCELLED)
                                and completedAt < today - r.statements
                                and not legalHold(run):
            if any child run (reprint, reload) of run is not final: continue    # still needed
            held = accountsUnderLegalHold(client)
            files = statementFiles(run) + outputFiles(run, kinds = r.outputKindsToPurge)
            files = [f for f in files if f.accountKey not in held]
            TX {
                insert purge_log(run, kinds, count(files), sum(sizes), today)    # written first
                delete run_record, statement rows for those accounts
                update run set purgedAt = today
            }
            for f in files: deleteFile(f)                  # after commit; a crash leaves only files to retry
        for file in select received_file where client = client.id and receivedAt < today - r.rawFiles
                                         and not referencedByUnpurgedRun(file):
            TX { insert purge_log(file, RAW_FILE, today); update received_file set purgedAt = today }
            deleteFile(file.storedPath)
    deleteOrphanFiles(olderThan = 7 days)                  # files on disk with no database row
    purgeAudit(olderThan = config.auditRetention)          # audit kept longest; chain checkpoint saved first
```

**Edge cases handled**

- The purge log row is written before anything is deleted, so even a crash mid-purge leaves a record of what was intended.
- A raw file is kept while any unpurged run still points at it, so a reprint can always re-map from the original.
- Audit records are deleted only after a checkpoint of the hash chain is saved, so the remaining chain can still be verified.
