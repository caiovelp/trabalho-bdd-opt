# trabalho-bdd-opt

### Schema

Crime dtypes:
 id                               int64
total_feminicide                 int64
total_homicide                   int64
total_felony_murder              int64
total_bodily_harm                int64
total_theft_cellphone            int64
total_armed_robbery_cellphone    int64
total_theft_auto                 int64
total_armed_robbery_auto         int64
segment_id                       int64
time_id                          int64
dtype: object

District dtypes:
 id           int64
name        object
geometry    object
dtype: object

Neighborhood dtypes:
 id           int64
name        object
geometry    object
dtype: object

Segment dtypes:
 id                    int64
geometry             object
oneway               object
length              float64
final_vertice_id      int64
start_vertice_id      int64
dtype: object

Time dtypes:
 id           int64
period      object
day        float64
month      float64
year         int64
weekday     object
dtype: object

Vertice dtypes:
 id                 int64
label              int64
district_id        int64
neighborhood_id    int64
zone_id            int64
dtype: object