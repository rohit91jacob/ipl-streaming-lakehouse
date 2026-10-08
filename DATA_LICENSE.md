# Data licence and attribution

**Ball-by-ball data: [Cricsheet](https://cricsheet.org/)**, made available under the
[Open Data Commons Attribution License (ODC-By) v1.0](https://opendatacommons.org/licenses/by/1-0/).

On 7 October 2026 Cricsheet put its JSON match data under ODC-By (its people register was already
ODC-By). The announcement is at
<https://cricsheet.org/article/licensing-and-venues-registry/>. Cricsheet's download archives now
include a `LICENSE.txt`. An unmodified copy ships at
[`tests/fixtures/cricsheet/LICENSE.txt`](tests/fixtures/cricsheet/LICENSE.txt).

What this repository contains and what it does not:

| Item | Licence |
|---|---|
| Source code, docs and configuration | MIT ([LICENSE](LICENSE)) |
| `tests/fixtures/cricsheet/*.json`: six unmodified match files from Cricsheet | ODC-By 1.0, © Cricsheet |
| `src/ipl_lakehouse/reference/match_adjustments.csv`: dates and teams of matches abandoned without a ball being bowled, which Cricsheet does not publish. Compiled from the cited Wikipedia season pages | Facts, cited per row |
| The full Cricsheet archive, and every table the pipeline builds from it | **Not committed.** `ipl ingest` downloads the archive at runtime. |

If you publish anything built with this pipeline, such as tables, dashboards or charts, ODC-By
requires you to credit Cricsheet and to say that the data is under ODC-By. For example:

> Contains ball-by-ball data from Cricsheet (https://cricsheet.org/), available under the Open Data
> Commons Attribution License v1.0.

"IPL" and the team names are trademarks of their owners. This project has no affiliation with the
BCCI, the IPL or Cricsheet.
