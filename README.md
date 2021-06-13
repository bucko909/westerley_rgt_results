# Westerley VR RGT Results Scraper

Scraper for rgtdb to get results for Westerley VR competition

Actual output data is in [out](out/).

* `{event_id}.csv` -- individual results for this race.
* `{event_id}_teams.csv` -- team results for this race.
* `{event_id}_users_cumulative.csv` -- individual results after this race.
* `{event_id}_teams_cumultaive.csv` -- team results after this race.
* `user_results.csv` -- final individual results (same as cumulative after the final race).
* `team_results.csv` -- final team results (same as cumulative after the final race).

Ingests the team members and events list in [data](data/). Should be as simple as adding a new event there and re-running to get latest results.
