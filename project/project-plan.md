# Project Plan

## Title
Physical Activity and Health in Europe

## Main Question

1. How does the time spent on physical activtiy impact mental and general health?

## Description

Over the last decades, the rate of people living a more sedentary lifestyle in Europe has increased drastically which may have adverse health effects. This project analyzes the correlation between the average time spent on physical activity and the health of the population in European countries. To evaluate the health of a population two indicators are examined. On the one hand, the percentage of people reporting depressive symptoms is considered as an indicator of mental health. On the other hand, a self-report on perceived health is used to assess general health. 

## Datasources

### Datasource1: Time spent on pyhsical activity
* Metadata URL: https://ec.europa.eu/eurostat/databrowser/view/hlth_ehis_pe2e$dv_300/default/table?lang=de
* Data URL: https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/HLTH_EHIS_PE2E/?format=SDMX-CSV&lang=de&label=label_only
* Data Type: CSV

Time spent on health-promoting (non-work-related) physical activity by country and year.

### Datasource2: Depressive symptoms
* Metadata URL: https://ec.europa.eu/eurostat/databrowser/view/hlth_ehis_mh1e/default/table?lang=de
* Data URL: https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/hlth_ehis_mh1e/?format=SDMX-CSV&lang=de&label=label_only
* Data Type: CSV

Current depressive symptoms by country and year.

### Datasource3: Self-report on general health
* Metadata URL: https://ec.europa.eu/eurostat/databrowser/view/hlth_silc_02/default/table?lang=de
* Data URL: https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/hlth_silc_02/?format=SDMX-CSV&lang=de&label=label_only
* Data Type: CSV

Self-report on helth by country and year


## Work Packages

1. Write Automated Data Pipeline [#1][i1]
2. Analyze Data Sources [#2][i2]
3. Clean Data [#3][i3]
4. Add Automated Test Cases [#4][i4]
5. Analyze Cleaned Data [#5][i5]
6. Write Report [#6][i6]

[i1]: https://github.com/leoniefaerber/made-template-ws2324/issues/5
[i2]: https://github.com/leoniefaerber/made-template-ws2324/issues/1
[i3]: https://github.com/leoniefaerber/made-template-ws2324/issues/2
[i4]: https://github.com/leoniefaerber/made-template-ws2324/issues/6
[i5]: https://github.com/leoniefaerber/made-template-ws2324/issues/3
[i6]: https://github.com/leoniefaerber/made-template-ws2324/issues/4