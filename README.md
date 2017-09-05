<p align="center">
    <img src="http://www.zavtech.com/morpheus/images/morpheus-logo.png"/>
</p>

### Introduction

The Morpheus library is designed to facilitate the development of high performance analytical software involving large datasets for 
both offline and real-time analysis on the [Java Virtual Machine](https://en.wikipedia.org/wiki/Java_virtual_machine) (JVM). The 
library is written in Java 8 with extensive use of lambdas, but is accessible to all JVM languages.

**For detailed documentation with examples, see [here](http://www.zavtech.com/morpheus/docs/)**

#### Motivation

At its core, Morpheus provides a versatile two-dimensional **memory efficient** tabular data structure called a `DataFrame`, similar to
that first popularised in [R](https://www.datacamp.com/community/tutorials/15-easy-solutions-data-frame-problems-r#gs.TjuEoj8). While
dynamically typed scientific computing languages like [R](https://www.r-project.org/), [Python](https://www.python.org/) & [Matlab](https://www.mathworks.com/products/matlab.html) 
are great for doing research, they are not well suited for large scale production systems as they become extremely difficult to maintain, 
and dangerous to refactor. The Morpheus library attempts to retain the power and versatility of the `DataFrame` concept, while providing a 
much more **type safe** and **self describing** set of interfaces, which should make developing, maintaining & scaling code complexity much 
easier. 

Another advantage of the Morpheus library is that it is extremely good at **scaling** on [multi-core processor](https://en.wikipedia.org/wiki/Multi-core_processor) 
architectures given the powerful [threading](https://en.wikipedia.org/wiki/Multithreading_(computer_architecture)) capabilities of the Java 
Virtual Machine. Many operations on a Morpheus `DataFrame` can seamlessly be run in **parallel** by simply calling `parallel()` on the entity 
you wish to operate on, much like with [Java 8 Streams](http://www.oracle.com/technetwork/articles/java/ma14-java-se-8-streams-2177646.html). 
Internally, these parallel implementations are based on the Fork & Join framework, and near linear improvements in performance are observed 
for certain types of operations as CPU cores are added.

#### Capabilities    

A Morpheus `DataFrame` is a column store structure where each column is represented by a Morpheus `Array` of which there are many 
implementations, including dense, sparse and [memory mapped](https://en.wikipedia.org/wiki/Memory-mapped_file) versions. Morpheus arrays 
are optimized and wherever possible are backed by primitive native Java arrays (even for types such as `LocalDate`, `LocalDateTime` etc...) 
as these are far more efficient from a storage, access and garbage collection perspective. Memory mapped Morpheus `Arrays`, while still 
experimental, allow very large `DataFrames` to be created using off-heap storage that are backed by files.

While the complete feature set of the Morpheus `DataFrame` is still evolving, there are already many powerful APIs to affect complex 
transformations and analytical operations with ease. There are standard functions to compute summary statistics, perform various types 
of [Linear Regressions](https://en.wikipedia.org/wiki/Linear_regression), apply [Principal Component Analysis](https://en.wikipedia.org/wiki/Principal_component_analysis) 
(PCA) to mention just a few. The `DataFrame` is indexed in both the row and column dimension, allowing data to be efficiently **sorted**, 
**sliced**, **grouped**, and **aggregated** along either axis.

#### Data Access

Morpheus also aims to provide a standard mechanism to load datasets from various data providers. The hope is that this API will 
be embraced by the community in order to grow the catalogue of supported data sources. Currently, providers are implemented to enable 
data to be loaded from [Quandl](https://www.quandl.com/), [The Federal Reserve](https://research.stlouisfed.org/fred2/), 
[The World Bank](http://www.worldbank.org/), [Yahoo Finance](http://finance.yahoo.com/) and [Google Finance](https://www.google.com/finance).

### Morpheus at a Glance

#### A Simple Example

Consider a dataset of motor vehicle characteristics accessible [here](http://zavtech.com/data/samples/cars93.csv).
The code below loads this CSV data into a Morpheus `DataFrame`, filters the rows to only include those vehicles that have a power 
to weight ratio > 0.1 (where *weight* is converted into kilograms), then adds a column to record the relative efficiency between highway 
and city mileage (MPG), sorts the rows by this newly added column in descending order, and finally records this transformed result 
to a CSV file.

<?prettify?>
```java
DataFrame.read().csv(options -> {
    options.setResource("http://zavtech.com/data/samples/cars93.csv");
    options.setExcludeColumnIndexes(0);
}).rows().select(row -> {
    double weightKG = row.getDouble("Weight") * 0.453592d;
    double horsepower = row.getDouble("Horsepower");
    return horsepower / weightKG > 0.1d;
}).cols().add("MPG(Highway/City)", Double.class, v -> {
    double cityMpg = v.row().getDouble("MPG.city");
    double highwayMpg = v.row().getDouble("MPG.highway");
    return highwayMpg / cityMpg;
}).rows().sort(false, "MPG(Highway/City)").write().csv(options -> {
    options.setFile("/Users/witdxav/cars93m.csv");
    options.setTitle("DataFrame");
});
```
  
This example demonstrates the functional nature of the Morpheus API, where many method return types are in fact a `DataFrame` and 
therefore allow this form of method chaining. In this example, the methods `csv()`, `select()`, `add()`, and `sort()` all return
a frame. In some cases the same frame that the method operates on, or in other cases a filter or shallow copy of the frame being
operated on. The first 10 rows of the transformed dataset in this example looks as follows, with the newly added column appearing
on the far right of the frame.

<pre class="frame">
 Index  |  Manufacturer  |     Model      |   Type    |  Min.Price  |   Price   |  Max.Price  |  MPG.city  |  MPG.highway  |       AirBags        |  DriveTrain  |  Cylinders  |  EngineSize  |  Horsepower  |  RPM   |  Rev.per.mile  |  Man.trans.avail  |  Fuel.tank.capacity  |  Passengers  |  Length  |  Wheelbase  |  Width  |  Turn.circle  |  Rear.seat.room  |  Luggage.room  |  Weight  |  Origin   |           Make            |  MPG(Highway/City)  |
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
     9  |      Cadillac  |       DeVille  |    Large  |    33.0000  |  34.7000  |    36.3000  |        16  |           25  |         Driver only  |       Front  |          8  |      4.9000  |         200  |  4100  |          1510  |               No  |             18.0000  |           6  |     206  |        114  |     73  |           43  |              35  |            18  |    3620  |      USA  |         Cadillac DeVille  |             1.5625  |
    10  |      Cadillac  |       Seville  |  Midsize  |    37.5000  |  40.1000  |    42.7000  |        16  |           25  |  Driver & Passenger  |       Front  |          8  |      4.6000  |         295  |  6000  |          1985  |               No  |             20.0000  |           5  |     204  |        111  |     74  |           44  |              31  |            14  |    3935  |      USA  |         Cadillac Seville  |             1.5625  |
    70  |    Oldsmobile  |  Eighty-Eight  |    Large  |    19.5000  |  20.7000  |    21.9000  |        19  |           28  |         Driver only  |       Front  |          6  |      3.8000  |         170  |  4800  |          1570  |               No  |             18.0000  |           6  |     201  |        111  |     74  |           42  |            31.5  |            17  |    3470  |      USA  |  Oldsmobile Eighty-Eight  |         1.47368421  |
    74  |       Pontiac  |      Firebird  |   Sporty  |    14.0000  |  17.7000  |    21.4000  |        19  |           28  |  Driver & Passenger  |        Rear  |          6  |      3.4000  |         160  |  4600  |          1805  |              Yes  |             15.5000  |           4  |     196  |        101  |     75  |           43  |              25  |            13  |    3240  |      USA  |         Pontiac Firebird  |         1.47368421  |
     6  |         Buick  |       LeSabre  |    Large  |    19.9000  |  20.8000  |    21.7000  |        19  |           28  |         Driver only  |       Front  |          6  |      3.8000  |         170  |  4800  |          1570  |               No  |             18.0000  |           6  |     200  |        111  |     74  |           42  |            30.5  |            17  |    3470  |      USA  |            Buick LeSabre  |         1.47368421  |
    13  |     Chevrolet  |        Camaro  |   Sporty  |    13.4000  |  15.1000  |    16.8000  |        19  |           28  |  Driver & Passenger  |        Rear  |          6  |      3.4000  |         160  |  4600  |          1805  |              Yes  |             15.5000  |           4  |     193  |        101  |     74  |           43  |              25  |            13  |    3240  |      USA  |         Chevrolet Camaro  |         1.47368421  |
    76  |       Pontiac  |    Bonneville  |    Large  |    19.4000  |  24.4000  |    29.4000  |        19  |           28  |  Driver & Passenger  |       Front  |          6  |      3.8000  |         170  |  4800  |          1565  |               No  |             18.0000  |           6  |     177  |        111  |     74  |           43  |            30.5  |            18  |    3495  |      USA  |       Pontiac Bonneville  |         1.47368421  |
    56  |         Mazda  |          RX-7  |   Sporty  |    32.5000  |  32.5000  |    32.5000  |        17  |           25  |         Driver only  |        Rear  |     rotary  |      1.3000  |         255  |  6500  |          2325  |              Yes  |             20.0000  |           2  |     169  |         96  |     69  |           37  |              NA  |            NA  |    2895  |  non-USA  |               Mazda RX-7  |         1.47058824  |
    18  |     Chevrolet  |      Corvette  |   Sporty  |    34.6000  |  38.0000  |    41.5000  |        17  |           25  |         Driver only  |        Rear  |          8  |      5.7000  |         300  |  5000  |          1450  |              Yes  |             20.0000  |           2  |     179  |         96  |     74  |           43  |              NA  |            NA  |    3380  |      USA  |       Chevrolet Corvette  |         1.47058824  |
    51  |       Lincoln  |      Town_Car  |    Large  |    34.4000  |  36.1000  |    37.8000  |        18  |           26  |  Driver & Passenger  |        Rear  |          8  |      4.6000  |         210  |  4600  |          1840  |               No  |             20.0000  |           6  |     219  |        117  |     77  |           45  |            31.5  |            22  |    4055  |      USA  |         Lincoln Town_Car  |         1.44444444  |
</pre>

#### A Regression Example

The Morpheus API includes a regression interface in order to fit data to a linear model using either [OLS](regression/ols/), 
[WLS](regression/wls/) or [GLS](regression/gls/). The code below uses the same car dataset introduced in the previous example, 
and regresses **Horsepower** on **EngineSize**. The code example prints the model results to standard out, which is shown below, 
and then creates a scatter chart with the regression line clearly displayed.

<?prettify?>
```java
//Load the data
DataFrame<Integer,String> data = DataFrame.read().csv(options -> {
    options.setResource("http://zavtech.com/data/samples/cars93.csv");
    options.setExcludeColumnIndexes(0);
});

//Run OLS regression and plot 
String regressand = "Horsepower";
String regressor = "EngineSize";
data.regress().ols(regressand, regressor, true, model -> {
    System.out.println(model);
    DataFrame<Integer,String> xy = data.cols().select(regressand, regressor);
    Chart.create().withScatterPlot(xy, false, regressor, chart -> {
        chart.title().withText(regressand + " regressed on " + regressor);
        chart.subtitle().withText("Single Variable Linear Regression");
        chart.plot().style(regressand).withColor(Color.RED).withPointsVisible(true);
        chart.plot().trend(regressand).withColor(Color.BLACK);
        chart.plot().axes().domain().label().withText(regressor);
        chart.plot().axes().domain().format().withPattern("0.00;-0.00");
        chart.plot().axes().range(0).label().withText(regressand);
        chart.plot().axes().range(0).format().withPattern("0;-0");
        chart.show();
    });
    return Optional.empty();
});
```

<pre class="frame">
==============================================================================================
                                   Linear Regression Results                                                            
==============================================================================================
Model:                                   OLS    R-Squared:                            0.5360
Observations:                             93    R-Squared(adjusted):                  0.5309
DF Model:                                  1    F-Statistic:                        105.1204
DF Residuals:                             91    F-Statistic(Prob):                  1.11E-16
Standard Error:                      35.8717    Runtime(millis)                           52
Durbin-Watson:                        1.9591                                                
==============================================================================================
   Index     |  PARAMETER  |  STD_ERROR  |  T_STAT   |   P_VALUE   |  CI_LOWER  |  CI_UPPER  |
----------------------------------------------------------------------------------------------
  Intercept  |    45.2195  |    10.3119  |   4.3852  |   3.107E-5  |    24.736  |   65.7029  |
 EngineSize  |    36.9633  |     3.6052  |  10.2528  |  7.573E-17  |    29.802  |   44.1245  |
==============================================================================================
</pre>

<p align="center">
    <img class="chart" src="http://www.zavtech.com/morpheus/docs/images/ols/data-frame-ols.png"/>
</p>

#### UK House Price Trends

It is possible to access all UK residential [real-estate transaction records](https://data.gov.uk/dataset/land-registry-monthly-price-paid-data)
from 1995 through to current day via the [UK Government Open Data](https://data.gov.uk/) initiative. The data is presented in CSV 
format, and contains numerous [columns](https://www.gov.uk/guidance/about-the-price-paid-data), including such information as the 
transaction date, price paid, fully qualified address (including postal code), property type, lease type and so on.

Let us begin by writing a function to load these CSV files from Amazon S3 buckets, and since they are stored one file per year,
we provide a parameterized function accordingly. Given the requirements of our analysis, there is no need to load all the columns in the 
file, so below we only choose to read columns at index 1, 2, 4, and 11. In addition, since the files do not include a header, we 
re-name columns to something more meaningful to make subsequent access a little clearer.

<?prettify?>
```java
/**
 * Loads UK house price from the Land Registry stored in an Amazon S3 bucket
 * Note the data does not have a header, so columns will be named Column-0, Column-1 etc...
 * @param year      the year for which to load prices
 * @return          the resulting DataFrame, with some columns renamed
 */
private DataFrame<Integer,String> loadHousePrices(Year year) {
    String resource = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-%s.csv";
    return DataFrame.read().csv(options -> {
        options.setResource(String.format(resource, year.getValue()));
        options.setHeader(false);
        options.setCharset(StandardCharsets.UTF_8);
        options.setIncludeColumnIndexes(1, 2, 4, 11);
        options.getFormats().setParser("TransactDate", Parser.ofLocalDate("yyyy-MM-dd HH:mm"));
        options.setColumnNameMapping((colName, colOrdinal) -> {
            switch (colOrdinal) {
                case 0:     return "PricePaid";
                case 1:     return "TransactDate";
                case 2:     return "PropertyType";
                case 3:     return "City";
                default:    return colName;
            }
        });
    });
}
```

Below we use this data in order to compute the median nominal price (not inflation adjusted) of an **apartment** for each year between 
1995 through 2014 for a subset of the largest cities in the UK. There are about 20 million records in the unfiltered dataset between 
1993 and 2014, and while it takes a fairly long time to load and parse (approximately 3.5GB of data), Morpheus executes the analytical 
portion of the code in about 5 seconds (not including load time) on a standard Apple Macbook Pro purchased in late 2013. Note how we use 
parallel processing to load and process the data by calling `results.rows().keys().parallel()`. 

<?prettify?>
```java
//Create a data frame to capture the median prices of Apartments in the UK'a largest cities
DataFrame<Year,String> results = DataFrame.ofDoubles(
    Range.of(1995, 2015).map(Year::of),
    Array.of("LONDON", "BIRMINGHAM", "SHEFFIELD", "LEEDS", "LIVERPOOL", "MANCHESTER")
);

//Process yearly data in parallel to leverage all CPU cores
results.rows().keys().parallel().forEach(year -> {
    System.out.printf("Loading UK house prices for %s...\n", year);
    DataFrame<Integer,String> prices = loadHousePrices(year);
    prices.rows().select(row -> {
        //Filter rows to include only apartments in the relevant cities
        final String propType = row.getValue("PropertyType");
        final String city = row.getValue("City");
        final String cityUpperCase = city != null ? city.toUpperCase() : null;
        return propType != null && propType.equals("F") && results.cols().contains(cityUpperCase);
    }).rows().groupBy("City").forEach(0, (groupKey, group) -> {
        //Group row filtered frame so we can compute median prices in selected cities
        final String city = groupKey.item(0);
        final double priceStat = group.colAt("PricePaid").stats().median();
        results.data().setDouble(year, city, priceStat);
    });
});

//Map row keys to LocalDates, and map values to be percentage changes from start date
final DataFrame<LocalDate,String> plotFrame = results.mapToDoubles(v -> {
    final double firstValue = v.col().getDouble(0);
    final double currentValue = v.getDouble();
    return (currentValue / firstValue - 1d) * 100d;
}).rows().mapKeys(row -> {
    final Year year = row.key();
    return LocalDate.of(year.getValue(), 12, 31);
});

//Create a plot, and display it
Chart.create().withLinePlot(plotFrame, chart -> {
    chart.title().withText("Median Nominal House Price Changes");
    chart.title().withFont(new Font("Arial", Font.BOLD, 14));
    chart.subtitle().withText("Date Range: 1995 - 2014");
    chart.plot().axes().domain().label().withText("Year");
    chart.plot().axes().range(0).label().withText("Percent Change from 1995");
    chart.plot().axes().range(0).format().withPattern("0.##'%';-0.##'%'");
    chart.plot().style("LONDON").withColor(Color.BLACK);
    chart.legend().on().bottom();
    chart.show();
});
```

The percent change in nominal median prices for **apartments** in the subset of chosen cities is shown in the plot below. It 
shows that London did not suffer any nominal house price decline as a result of the Global Financial Crisis (GFC), however not 
all cities in the UK proved as resilient. What is slightly surprising is that some of the less affluent northern cities saw a 
higher rate of appreciation in the 2003 to 2006 period compared to London. One thing to note is that while London did not see 
any nominal price reduction, there was certainly a fairly severe correction in terms of EUR and USD since Pound Sterling 
depreciated heavily against these currencies during the GFC.

<p align="center">
    <img class="chart" src="http://www.zavtech.com/morpheus/docs/images/uk-house-prices.png"/>
</p>

### Visualization

Visualizing data in Morpheus `DataFrames` is made easy via a **simple chart abstraction API** with adapters supporting both 
[JFreeChart](http://www.jfree.org/jfreechart/) as well as [Google Charts](https://developers.google.com/chart/) (with others
to follow by popular demand). This design makes it possible to generate interactive [Java Swing](https://en.wikipedia.org/wiki/Swing_(Java)) 
charts as well as HTML5 browser based charts via the same programmatic interface. For more details on how to use this API, 
see the section on visualization [here](./viz/charts/overview/), and the code [here](https://github.com/zavtech/morpheus-viz).

<table width="100%" border="0">
    <tr style="background-color:white;">
        <td style="background-color:white;"><img class="smallChart" src="http://www.zavtech.com/morpheus/docs/images/gallery/chart-1.png"/></td>
        <td style="background-color:white;"><img class="smallChart" src="http://www.zavtech.com/morpheus/docs/images/gallery/chart-2.png"/></td>
    </tr>
    <tr style="background-color:white;">
        <td style="background-color:white;"><img class="smallChart" src="http://www.zavtech.com/morpheus/docs/images/gallery/chart-3.png"/></td>
        <td style="background-color:white;"><img class="smallChart" src="http://www.zavtech.com/morpheus/docs/images/gallery/chart-4.png"/></td>
    </tr>
    <tr style="background-color:white;">
        <td style="background-color:white;"><img class="smallChart" src="http://www.zavtech.com/morpheus/docs/images/gallery/chart-5.png"/></td>
        <td style="background-color:white;"><img class="smallChart" src="http://www.zavtech.com/morpheus/docs/images/gallery/chart-6.png"/></td>
    </tr>
    <tr style="background-color:white;">
        <td style="background-color:white;"><img class="smallChart" src="http://www.zavtech.com/morpheus/docs/images/gallery/chart-7.png"/></td>
        <td style="background-color:white;"><img class="smallChart" src="http://www.zavtech.com/morpheus/docs/images/gallery/chart-8.png"/></td>
    </tr>
    <tr style="background-color:white;">
        <td style="background-color:white;"><img class="smallChart" src="http://www.zavtech.com/morpheus/docs/images/gallery/chart-9.png"/></td>
        <td style="background-color:white;"><img class="smallChart" src="http://www.zavtech.com/morpheus/docs/images/gallery/chart-10.png"/></td>
    </tr>
    <tr style="background-color:white;">
        <td style="background-color:white;"><img class="smallChart" src="http://www.zavtech.com/morpheus/docs/images/gallery/chart-11.png"/></td>
        <td style="background-color:white;"><img class="smallChart" src="http://www.zavtech.com/morpheus/docs/images/gallery/chart-12.png"/></td>
    </tr>
    <tr style="background-color:white;">
        <td style="background-color:white;"><img class="smallChart" src="http://www.zavtech.com/morpheus/docs/images/gallery/chart-13.png"/></td>
        <td style="background-color:white;"><img class="smallChart" src="http://www.zavtech.com/morpheus/docs/images/gallery/chart-14.png"/></td>
    </tr>
    <tr style="background-color:white;">
        <td style="background-color:white;"><img class="smallChart" src="http://www.zavtech.com/morpheus/docs/images/gallery/chart-15.png"/></td>
        <td style="background-color:white;"><img class="smallChart" src="http://www.zavtech.com/morpheus/docs/images/gallery/chart-16.png"/></td>
    </tr>
    <tr style="background-color:white;">
        <td style="background-color:white;"><img class="smallChart" src="http://www.zavtech.com/morpheus/docs/images/gallery/chart-17.png"/></td>
        <td style="background-color:white;"><img class="smallChart" src="http://www.zavtech.com/morpheus/docs/images/gallery/chart-18.png"/></td>
    </tr>
    <tr style="background-color:white;">
        <td style="background-color:white;"><img class="smallChart" src="http://www.zavtech.com/morpheus/docs/images/gallery/chart-19.png"/></td>
        <td style="background-color:white;"><img class="smallChart" src="http://www.zavtech.com/morpheus/docs/images/gallery/chart-20.png"/></td>
    </tr>
    <tr style="background-color:white;">
        <td style="background-color:white;"><img class="smallChart" src="http://www.zavtech.com/morpheus/docs/images/gallery/chart-21.png"/></td>
        <td style="background-color:white;"><img class="smallChart" src="http://www.zavtech.com/morpheus/docs/images/gallery/chart-22.png"/></td>
    </tr>
</table>

### Maven Artifacts

Morpheus is published to Maven Central so it can be easily added as a dependency in your build tool of choice. The codebase is currently
divided into 5 repositories to allow each module to be evolved independently. The core module, which is aptly named [morpheus-core](https://github.com/zavtech/morpheus-core),
is the foundational library on which all other modules depend. The various Maven artifacts are as follows: 

**Morpheus Core**

The [foundational](https://github.com/zavtech/morpheus-core) library that contains Morpheus Arrays, DataFrames and other key interfaces & implementations.

```xml
<dependency>
    <groupId>com.zavtech</groupId>
    <artifactId>morpheus-core</artifactId>
    <version>${VERSION}</version>
</dependency>
```

**Morpheus Visualization**

The [visualization](https://github.com/zavtech/morpheus-viz) components to display `DataFrames` in charts and tables.

```xml
<dependency>
    <groupId>com.zavtech</groupId>
    <artifactId>morpheus-viz</artifactId>
    <version>${VERSION}</version>
</dependency>
```

**Morpheus Quandl**

The [adapter](https://github.com/zavtech/morpheus-quandl) to load data from [Quandl](http://www.quandl.com)

```xml
<dependency>
    <groupId>com.zavtech</groupId>
    <artifactId>morpheus-quandl</artifactId>
    <version>${VERSION}</version>
</dependency>
```

**Morpheus Google**

The [adapter](https://github.com/zavtech/morpheus-google) to load data from [Google Finance](http://finance.google.com)

```xml
<dependency>
    <groupId>com.zavtech</groupId>
    <artifactId>morpheus-google</artifactId>
    <version>${VERSION}</version>
</dependency>
```

**Morpheus Yahoo**

The [adapter](https://github.com/zavtech/morpheus-yahoo) to load data from [Yahoo Finance](http://finance.yahoo.com)

```xml
<dependency>
    <groupId>com.zavtech</groupId>
    <artifactId>morpheus-yahoo</artifactId>
    <version>${VERSION}</version>
</dependency>
```

### Q&A Forum

A Questions & Answers forum has been setup using Google Groups and is accessible [here](https://groups.google.com/forum/#!forum/morpheus-lib)

### Javadocs

Morpheus Javadocs can be accessed online [here](http://www.zavtech.com/morpheus/api).

### Build Status

A Continuous Integration build server can be accessed [here](http://zavnas.com/jenkins/), which builds code after each merge.

### License

Morpheus is released under the [Apache Software Foundation License Version 2](https://www.apache.org/licenses/LICENSE-2.0).
