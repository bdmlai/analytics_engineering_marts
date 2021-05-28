Database: Analytics Workspace;Schema: CONTENT


| Category     | Materialisation | Table                                                | Description                                                                    | Business Unit |
| ------------ | ----------------| -----------------------------------------------------| -------------------------------------------------------------------------------| ------------- |
| Intermediate | Ephemeral       | intm\_weekly\_international\_tagging\_aew            | Intermediate Model to capture records belonging to aew                         | Content       |
| Intermediate | Ephemeral       | intm\_weekly\_domestic\_tagging\_aaa                 | Intermediate Model to capture records belonging to mlb                         | Content       |
| Intermediate | Ephemeral       | intm\_weekly\_domestic\_tagging\_impactwrestling     | Intermediate Model to capture records belonging to nascar                      | Content       |
| Intermediate | Ephemeral       | intm\_weekly\_domestic\_tagging\_cmll                | Intermediate Model to capture records belonging to nba                         | Content       |
| Summary      | Table           | rpt\_weekly\_international\_tagging                  | Summary table to capture tagged international data of wrestling properties     | Content       |

