Database: Analytics Workspace;Schema: CONTENT


| Category     | Materialisation | Table                                    | Description                                                | Business Unit |
| ------------ | ----------------| ---------------------------------------- | ---------------------------------------------------------- | ------------- |
| Base         | View            | base\_weekly\_domestic\_tagging                | Base Model containing data of all properties 2018 onwards  | Content       |
| Intermediate | Ephemeral       | intm\_weekly\_domestic\_tagging\_aew           | Intermediate Model to capture records belonging to aew     | Content       |
| Intermediate | Ephemeral       | intm\_weekly\_domestic\_tagging\_mlb           | Intermediate Model to capture records belonging to mlb     | Content       |
| Intermediate | Ephemeral       | intm\_weekly\_domestic\_tagging\_nascar        | Intermediate Model to capture records belonging to nascar  | Content       |
| Intermediate | Ephemeral       | intm\_weekly\_domestic\_tagging\_nba           | Intermediate Model to capture records belonging to nba     | Content       |
| Intermediate | Ephemeral       | intm\_weekly\_domestic\_tagging\_nfl           | Intermediate Model to capture records belonging to nfl     | Content       |
| Intermediate | Ephemeral       | intm\_weekly\_domestic\_tagging\_nhl           | Intermediate Model to capture records belonging to nhl     | Content       |
| Summary      | Table           | rpt\_weekly\_domestic\_tagging                 | Summary table to capture tagged data of all properties| Content       |

