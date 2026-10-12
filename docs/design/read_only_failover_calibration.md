# Read-Only Failover for Calibration Home

Branch: `ewm18395_read_only_failover_calibration`
Status: assessment / design agreed, not yet implemented (updated 2026-10-09)

## Goal

Allow dev (`next`) and `qa` deployments to **read** prod calibration data for testing, while guaranteeing they
never **write** to prod. All writes continue to go to the configured `instrument.calibration.home`. Failover
locations are supplied through the application yml.

This is a **testing-only** feature. It is not available in prod deployments, and it is not intended to encourage
users to maintain several calibration folders.

## Agreed decisions

1. **Scope: calibration folders only.** The layering abstraction (`CalibrationRoot`, below) exists solely to
   overlay whole calibration folders. It is not a general-purpose storage layer and will not be reused for other
   homes (e.g. reduction). The structural unit within a calibration folder is never fragmented across roots.
2. **Failover granularity: per indexer directory.** For a given `<state>/<mode>/<workflow>` directory, use home if
   its index exists, otherwise the first failover root that has one. Shadowing only — no merged/overlay view of
   versions. Everything for that directory (index, records, parameters, workspaces) is read from the same root.
   **The default version is ignored when choosing the root** (decided 2026-10-12): an index containing only the
   default version (e.g. a state that was only initialized in home) does not shadow a failover. The first root
   whose index has a non-default version is read; if none has one, the first root that has an index (home first)
   is read, i.e. home's own default. Writes to a state that doesn't exist in home first initialize it in home;
   after that, failover continues as usual until home has its first non-default version. Once dev writes that
   version, prod's versions of that state are no longer visible to dev (document this; `copyCalibration` can be
   used to import prod history if needed). Not applied to instrument parameters, whose version 0 is their first
   epoch rather than a default.
3. **Resolve by existence, not by retry-on-failure.** A root is skipped only when the target is *missing*. Parse
   errors, permission errors, or corrupted files in home must surface, never silently fall through to a failover.
4. **Marker file** at the root of every deployed calibration home, holding the calibration folder schema version
   (must match a new application.yml field) and the intended deploy environment.
5. **Environment values:** `next | qa | prod | exp`. `next | qa | prod` match `DeployEnvEnum` in
   `src/snapred/meta/Config.py` (only the commented header in `snapred_next.yml` says "dev"); `exp` is new and is
   deliberately not added to the enum (see "Delivery").
6. **Failover with schema mismatch (or missing marker): warn and continue** — skip that root.
7. **Schema version bumps are manual.** An admin migrates the folder and re-stamps it. No automatic migration
   tooling for now. Whoever bumps `schemaVersion` in application.yml must re-stamp the target deployed folders
   before that release ships, otherwise the deploy hard-fails on its own home (intended).
8. **No `user` environment.** Marker checks exist only to stop deployed software from touching the wrong deployed
   folder. User homes (`snapred-user.yml`) are exempt from the home marker check and are not stamped.
9. **Prod guard.** Failover is refused in prod deployments (see "Prod guard" below).

## Configuration

**`Config` is not modified by this feature.** `src/snapred/meta/Config.py` is tech debt slated to be replaced by
`neutrons_standard.config` (https://github.com/neutrons/neutronsStandard), so everything here is done with yml
changes plus SNAPRed-side code that only uses the existing public read API (`Config[...]`, `Config.exists`,
`Config.snapredVersion()`). No new `validate()` rules, no new flags, no changes to interpolation or user-yml
generation.

```yaml
# application.yml
instrument:
  calibration:
    home: ${instrument.home}/shared/Calibration
    schemaVersion: 1        # bump manually when folder layout / index format changes
    environment: prod       # overridden per env yml
    readFailover:           # testing only
      enabled: false        # must stay false for prod

# snapred_qa.yml
instrument:
  calibration:
    home: ${instrument.home}/shared/CalibrationQA
    environment: qa
    readFailover:
      enabled: true
      paths:                # mapping, not a list: see notes
        prod: ${instrument.home}/shared/Calibration
```

Notes:

- **Read order, no defaults.** Code checks whether failover is on before it touches any other failover key:
  1. prod deployment (version-detected) or user mode: failover off; no `readFailover.*` key is read at all.
  2. otherwise read `instrument.calibration.readFailover.enabled`; if `false`, stop.
  3. only then read `instrument.calibration.readFailover.paths`.
  `enabled: true` with no `paths` simply raises `Config`'s normal `KeyError`; no extra validation is added.
- **`paths` is a mapping of name -> path, not a list.** `Config._replace` does not interpolate list elements
  (same in `neutrons_standard`), but `Config.__getitem__` on a mapping recursively resolves each member, so
  `Config["instrument.calibration.readFailover.paths"]` returns interpolated paths with no `Config` change.
  Priority is yml order (dict insertion order is preserved). The names also make log messages clearer.
- Sanity checks (failover equal to, containing, or contained by `home`) live in `CalibrationRoot` construction,
  not `Config.validate()`.
- **User-mode detection without a new flag:** `_generateUserYml` writes `environment: snapred-user` into the user
  yml (`{package_name}-user` upstream), and `refresh` keeps an explicit `environment`. So
  `Config["environment"].endswith("-user")` identifies user mode through the public API. Note: the deployed
  `instrument.calibration.environment` still carries over into user mode, so check the top-level `environment`,
  not the calibration one.
- Derived keys (`powder.home`, `powder.grouping.home`, `sample.home`, `parameters.home`, `lite.map.file`,
  `instrument.config`, `samples.home`) remain in the yml as the home layer's layout, but calibration code stops
  reading them directly. Code addresses files as `CalibrationRoot` + relative subpath (`Powder/...`,
  `CalibrantSamples/...`, `SNAPInstPrm/...`). Expect test churn where tests `Config_override` derived keys.

## Prod guard

Failover must not be usable in prod, and must not be something users are nudged toward. All checks live in
SNAPRed code (`CalibrationRoot` construction, run at startup), not in `Config`.

- **Authoritative prod detection comes from the installed version, not the yml.** Use the same rule as
  `Config.configureForDeploy`: a version containing `dev` is `next`, `rc` is `qa`, otherwise `prod`. Read via
  `Config.snapredVersion()` (renamed `packageVersion()` in `neutrons_standard`; wrap it in one helper so the
  migration touches a single line). A user yml cannot opt a prod install into failover by setting
  `environment: qa`.
- In a prod deployment, `readFailover.enabled: true` is a **hard error at startup** (not a warning), regardless of
  which yml supplied it. Also reject it if the configured `instrument.calibration.environment` is `prod`.
- **User mode never uses failover.** Step 1 of the read order short-circuits before any `readFailover.*` key is
  read, so the deployed QA/next value that `_generateUserYml` copies into `snapred-user.yml` is inert.
- Not documented in user-facing docs; documented only for dev/qa deployment configuration.
- Test environments (`isTestEnv()`) are allowed.

## Deferred: generic user-yml key blacklist

Not needed for this feature (user mode ignores `readFailover` entirely), and it would require changing `Config`.
If wanted later, so that copied-but-inert keys like `readFailover` don't even appear in a generated
`snapred-user.yml`, propose it upstream in `neutrons_standard` as a generic, configurable feature:

- configurable list of dotted keys (e.g. `application.userYml.excludedKeys`), never key-specific code;
- applied when generating the user yml, and to the merged config after a user yml is loaded;
- the list itself read from the deployed config only, not overridable by the user yml.

## Marker file

`<home>/calibrationHome.json` (deliberately not a dotfile — easy for admins to find), backed by a pydantic DAO:

```json
{ "schemaVersion": 1, "environment": "prod", "createdBy": "...", "createdAt": "..." }
```

### Validation rules

| Root     | schemaVersion mismatch     | environment mismatch                             | marker missing                 |
|----------|----------------------------|--------------------------------------------------|--------------------------------|
| home     | hard fail (no read/write)  | hard fail (catches `home` misconfigured to prod) | hard fail (after grace period) |
| failover | warn, skip root            | expected; recorded for the write guard           | warn, skip root                |

- User mode: home marker check skipped; failover rules and the write guard still apply.
- Write guard: refuse to write under any root whose marker env differs from the configured `environment`
  (independent of path routing).

### Migration

- **Never auto-stamp an existing non-empty home** — a misconfigured QA install would stamp prod as `qa` and defeat
  the guard.
- Provide an explicit admin tool, e.g.
  `python -m snapred.tools.stampCalibrationHome --home <path> --env prod --schema 1`, refusing non-empty dirs
  without `--force`.
- Optional grace flag `instrument.calibration.requireMarker: false`: missing home marker gives a warning instead
  of failing. Flip to `true` once prod, qa and next homes are stamped.

## Read interface: `CalibrationRoot`

Rather than teaching every call site about failover, all calibration-folder access goes through one overlay
interface. Home is the top layer, valid failovers below it; reads resolve top-down, writes only ever touch home.
Behaviour is analogous to Mantid's datasearch directories or an overlay filesystem.

Why resolve-by-existence instead of catch-and-retry:

- Retry-on-exception would mask corruption in home by silently serving prod data.
- Indexed data must stay in one root; per-file retry would mix home's index with prod's version folders and break
  `reconcileIndexToFiles`.
- Most heavy reads are Mantid algorithms (`LoadNexus`, `LoadDiffCal`, grouping loads) and `parse_file_as(path)`,
  which take paths. Failures inside an algorithm can't be cleanly caught and retried, so the interface hands out a
  **resolved path** before the read happens.

```python
class CalibrationRoot:
    """Overlay of [home, *validFailovers]. Reads resolve top-down; writes only to home."""

    def __init__(self, home: Path, failovers: list[Path]): ...
        # validates markers + schema, applies the prod guard

    # single-file reads
    def resolve(self, rel: Path) -> Path            # first root where it exists, else FileNotFoundError listing roots tried
    def exists(self, rel: Path) -> bool

    # listing reads (findCompatibleStates, samples)
    def glob(self, rel_pattern: str) -> list[Path]  # union, home wins on name collision
    def listDirs(self, rel: Path) -> list[str]

    # indexed reads
    def view(self, rel: Path, anchor: str) -> DirView  # pinned to first root containing `anchor` (the index file)

    # writes
    def writePath(self, rel: Path) -> Path          # always home; runs the write guard; the single chokepoint


class DirView:
    readRoot: Path    # re-pins lazily: moves to home once home contains the anchor
    writeRoot: Path   # always home/<rel>
```

Case mapping:

- **Single file** (`lite.map.file`, default grouping map, CIF, samples): `resolve()`.
- **Indexed** (calibration, normalization, instrument parameters):
  `Indexer(view=root.view(statePath, "CalibrationIndex.json"))`. The Indexer reads via `view.readRoot` and
  writes/locks via `view.writeRoot`.
- **Listing** (`findCompatibleStates`, `readSampleFilePaths`): `glob()` / `listDirs()`.
- **Writes**: all via `writePath()`, so `assertWritable` lives in one place.

Benefits:

- Failover logic and the write guard live in one module, unit-testable against tmp dirs.
- Marker/schema validation and the prod guard live in `CalibrationRoot` construction; no separate service.
- `DirView` lazy re-pinning fixes the `lru_cache` staleness problem; indexer caching can stay.
- Call-site migration is mechanical: `Path(Config["instrument.calibration.powder.home"], x)` becomes
  `calibrationRoot.resolve(...)` / `writePath(...)`.

## Code touch points

### `Indexer` (`src/snapred/backend/data/Indexer.py`)

- Constructed from a `DirView` instead of a bare `directory`.
- Read via `readRoot`: `readIndex`, `readDirectoryList`, `readIndexedObject`, read side of `versionPath`.
- Write via `writeRoot`: `writeIndex`, `writeIndexedObject`, `recoverIndex`, `obtainLock`, `_lockContext` (locks
  on write root only).
- `nextVersion` / `currentVersion` must use the write root.
- `versionPath` is used by both readers (GroceryService workspace loads) and writers
  (`writeCalibrationWorkspaces`, `copyCalibration`); split into read/write variants and audit callers.

### `LocalDataService` (`src/snapred/backend/data/LocalDataService.py`)

- Owns the `CalibrationRoot`; rebuild it on Config reload / `swapToUserYml`.
- **Highest risk — writing to a state that exists only in a failover.** Writes go to home, and the home state must
  first be initialized (default version + `groupingMap.json`, i.e. the `initializeState` / `_prepareStateRoot`
  logic), otherwise home shadows prod with an incomplete state. This is domain logic and stays here.
- `readStateConfig` calls `_prepareStateRoot` as a side effect of a read when `groupingMap.json` is missing; switch
  to `root.exists()` so stub state dirs are not created in home.
- `findCompatibleStates`: `root.listDirs("Powder")`.
- `checkCalibrationFileExists`, `_readGroupingMap`, `_defaultGroupingMapPath`: `exists()` / `resolve()`.
- `readSampleFilePaths`: `glob()`. `readCifFilePath`: `resolve()`.
- Home-only writes via `writePath()`: `writeCalibrantSample`, `_writeGroupingMap`, `writeInstrumentParameters`,
  `writeWorkspace`, `writeDiffCalWorkspaces`, `writePixelMask`. Mantid save algorithms (`SaveNexus`,
  `SaveDiffCal`) must receive paths from `writePath()` — enforced by convention and review.
- `readInstrumentConfig`: keep requiring `home` to exist.

### DAO path resolution

- `GroupingMap._asAbsolutePath` resolves relative definitions against `grouping.home`; resolve each definition via
  `CalibrationRoot.resolve()` instead.
- `Crystallography.calibrationSampleHome`: same.

### `GroceryService`

- Mostly via `indexer.versionPath` — covered by the Indexer change.
- `Config["instrument.lite.map.file"]`: `resolve()`.

### `Config` (`src/snapred/meta/Config.py`)

- **No changes.** Only new yml keys in `application.yml` and the env ymls. Keeps the eventual migration to
  `neutrons_standard.config` unaffected.

### Cosmetic

- `__main__.py` (~line 123) and `ContinueWarning.py` (~line 50): show failover roots too (dev/qa only).

## Write-to-prod enforcement

1. **App guard:** `CalibrationRoot.writePath()` — rejects paths that `resolve()` under a failover root and roots
   whose marker env doesn't match the configured environment.
2. **Real guarantee:** filesystem permissions. Dev/QA service accounts should have read-only access to the prod
   calibration home. Recommended as a requirement; the app guard alone cannot stop a stray Mantid algorithm.

## Tests

- ~78 test references to `calibrationIndexer` / `normalizationIndexer` / calibration-home overrides; most mock the
  indexer. Indexer unit tests need `DirView` cases.
- Test and integration ymls need `schemaVersion` / `environment`; the `calibration_home_from_mirror` fixture and
  test data need a marker (possible cross-repo change if the mirror is external).
- New cases:
  - `CalibrationRoot`: resolve top-down; missing-only fallthrough (corrupt home file is *not* skipped); glob union
    with home winning; `DirView` pinning and lazy re-pin
  - writes never land in a failover (guard + tmp-dir assertion)
  - write to a failover-only state initializes home
  - `readFailover.paths` mapping resolves interpolated paths in yml order
  - read order: prod / user mode never read `readFailover.*`; `enabled: false` never reads `paths`
  - user-mode detection via top-level `environment` ending in `-user`
  - prod guard: version-detected prod with `readFailover.enabled: true` fails at startup, even with
    `environment: qa` set in yml; `CalibrationRoot` construction refuses in prod
  - home schema mismatch / env mismatch fails; failover schema mismatch skipped with warning
  - user mode skips home marker check but still guards failovers
  - stamp tool refuses non-empty dir without `--force`

## Delivery: `exp` branch and environment

Most of this work lands on a new long-lived `exp` branch, deployed as its own environment, so `qa` stays free for
bug-fix releases. Failover is intended to be enabled in `exp` (assumption: `qa` and `next` keep
`readFailover.enabled: false` unless decided otherwise). `exp` in the version string means the `exp` env.

Two repos are involved:

- **SNAPRed** builds the `snapred` conda package. Its env yml is baked in at build time (`build-configure-app` ->
  `python -m snapred --configure`), so an `exp` snapred package must be built configured for `exp`.
- **SNAPWrap** is what is mainly deployed. Its deployed env pins a `snapred` package, so it must pull the `exp`
  snapred build, and it has its own CI and GitLab pixi-deploy repos.

### Version string

- Exp releases are **tagged like rc's**: `v<major>.<minor>.<patch>exp<n>[-<name>]`, e.g.
  `v2.4.0exp1-cycle-aware-calib`. `<name>` identifies the experimental feature being developed for historic
  reference; the real `exp` feature is cycle-aware calibration (read-only failover exists to make testing it
  against prod data possible). Also use annotated tags (`git tag -a ... -m "<description>"`). Only tags are
  published; untagged `exp` branch builds are not.
- PEP 440 does not allow `exp` as a pre-release segment (hatchling raises `Invalid version`), so a custom
  versioningit `tag2version` (`tools/versioning.py`) maps `2.4.0exp1-cycle-aware-calib` ->
  `2.4.0+exp.1.cycle.aware.calib` (local version label). The number is its own segment so it orders
  numerically (`exp.10 > exp.2`); name separators become `.` (conda forbids `-` in versions). "exp" stays in the
  version string for `--configure` detection. `build-configure-app` runs `versioningit --write` first so a stale
  editable `_version.py` can't make a tagged exp build configure as next.
- **Matching hazard** (checked with conda's `VersionOrder` / `VersionSpec`): `2.4.0+exp1` does *not* sort above
  `2.4.0` and is not matched by `>=2.4.0` or `==2.4.0`, but it **is** matched by `2.4.0.*` and `~=2.4.0rc` (the
  form used by the snapwrap deploy repos). Python/PEP 440 ordering differs (`2.4.0+exp1 > 2.4.0`). Either way,
  exp builds visible to a qa/prod/next env can be picked up. Exp builds must be published under their **own anaconda
  label** (e.g. `neutrons/label/exp`) that no qa/prod/next env lists as a channel.

### SNAPRed changes

- `src/snapred/resources/snapred_exp.yml`:
  ```yaml
  # environment: exp
  instrument:
    calibration:
      home: ${instrument.home}/shared/CalibrationExp
      environment: exp
      readFailover:
        enabled: true
        paths:
          prod: ${instrument.home}/shared/Calibration
  ```
- **Build-time env selection without touching `Config`:** `Config.configureForDeploy()` checks `dev`, then `rc`,
  else prod. `2.4.0+exp1` would configure as **prod**, and `2.4.0.dev3+exp` as **next**. In `__main__.py`, before
  falling back to `configureForDeploy()`: if the version contains `exp` (or an explicit `--configure <env>` /
  CI env var is given), call the existing public `Config.mergeAndExport("snapred_exp")`. `DeployEnvEnum` is not
  extended.
- **Prod-guard helper** (SNAPRed-side, wraps `Config.snapredVersion()`): check `exp` first, then `dev`, then `rc`,
  else prod. An exp build must never be classified as prod (it would refuse failover) or as next.
- **Marker env values** become `next | qa | prod | exp`.
- **CI (`.github/workflows/package.yaml`):**
  - `push.branches` is `[main]`; add `exp`. Make `exp` protected (`trigger-deploy` requires `ref_protected`).
  - Anaconda upload picks label `rc` if the ref contains `rc`, else **`main`**. An exp tag would currently be
    uploaded to `main`. Add an `exp` label branch to that logic (check `exp` before the main fallback).
  - `neutrons/branch-mapper@v2` only distinguishes release tags, `rc` tags, and everything else; see below.
- **pixi (`pyproject.toml`):** optional `exp` pixi environment if exp needs different Mantid pins than `dev`.

### SNAPWrap changes (`~/git/SNAPWrap`)

- `exp` branch; add it to `push.branches` in `.github/workflows/test_and_deploy.yaml` (currently
  `[next, qa, main]`).
- `publish` job runs for tags or `refs/heads/next`; add exp tags/branch, publishing to the `exp` label.
  `neutrons/conda-actions/publish@main` decides the label internally; confirm it can target `exp` (likely another
  shared-action change). Same for `pkg-remove`'s `CONDA_LABEL`.
- Add a `deploy-exp` job mirroring `deploy-dev`, triggering a new GitLab deploy project.
- `pyproject.toml` pins `snapred = "==2.3.1"` (both `dependencies` and `package.run-dependencies`) and lists
  `neutrons` / `neutrons/label/rc` channels. On `exp`, pin the exp snapred build and add the `neutrons/label/exp`
  channel there only.
- SNAPWrap has no own deploy-env config logic (no `configureForDeploy` use); the calibration env comes entirely
  from the snapred package it installs, hence the SNAPRed build-time selection above.

### Deploy repos (GitLab, `sns-hfir-scse/deployments/pixi-envs`)

- New `snapwrap_exp-pixi-deploy`, cloned from `snapwrap_dev-pixi-deploy` (`~/git/snapwrap_dev-pixi-deploy`):
  - `prod/default/pixi.toml`: `name = "snapwrap_exp"`; channels include `neutrons/label/exp`; pin the exp
    `snapwrap` and exp `snapred` builds.
  - `.gitlab-ci.yml`: `MY_ENV: "snapwrap_exp"`.
  - Its GitLab project id goes into SNAPWrap's `deploy-exp` trigger.
- Confirm existing `snapwrap_qa` / `snapwrap_dev` / prod deploy repos do **not** list `neutrons/label/exp`.
- SNAPRed's own `trigger-deploy` targets GitLab project 7835 with `CONDA_ENV` from branch-mapper; add a
  `snapred_exp` env there too if SNAPRed is still deployed standalone.
- Whatever exposes envs to users (launcher/module/desktop entry) needs an exp entry.

### `neutrons/branch-mapper`

Shared action used across neutrons repos; maps `github.ref` to `<prefix>_<suffix>` with inputs `suffix-release`
(`v*`), `suffix-release-candidate` (`v*rc*`) and `suffix-default`. To support exp, add a backward-compatible
optional input (e.g. `suffix-experimental`, matching `+exp` tags and/or `refs/heads/exp`), checked before the rc
and release rules, and only active when set, so other repos are unaffected. Alternative: skip branch-mapper for
exp refs with an inline workflow step that sets the env name.

### Calibration home and branch hygiene

- New `${instrument.home}/shared/CalibrationExp`: with failover enabled, the skeleton (`SNAPInstPrm`,
  `CalibrantSamples`, `Powder/PixelGroupingDefinitions`, `LiteGroupMap.hdf`) can resolve through to prod, so the
  home can start as an empty, stamped (`exp`) directory. It must exist (`readInstrumentConfig` requires it) and be
  writable by the exp deploy account; prod should be read-only for that account.
- Merge `next` into `exp` regularly in both repos. The generic pieces (`CalibrationRoot`, marker, stamp tool) can
  go to `next` once proven; `snapred_exp.yml` and the exp CI/deploy wiring stay exp-specific.

## Implementation order

1. yml keys (no `Config` code changes).
2. `CalibrationRoot` + `DirView` + marker DAO + write guard + prod guard + user-mode detection, with standalone
   tests.
3. Stamp tool.
4. Indexer moves to `DirView`.
5. Migrate `LocalDataService` / DAO / GroceryService call sites (fix read-with-write side effects first).
6. Tests, test ymls, mirror marker.
7. `exp` delivery (SNAPRed): `snapred_exp.yml`, `__main__` exp-aware configure, exp anaconda label, CI branch
   wiring.
8. `exp` delivery (shared/other repos): branch-mapper + conda-actions exp support, SNAPWrap exp branch/CI/pins,
   new `snapwrap_exp-pixi-deploy`, create + stamp `CalibrationExp`.
9. Ops step: stamp prod, qa, next and exp homes before release.

Steps 1–3 are independently reviewable as a first PR.

Estimated effort: ~2–2.5 weeks including tests.
