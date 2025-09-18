
%Macro ETL_Project;

PROC IMPORT OUT=work.sales DATAFILE="/home/u61627519/Udemy_practice/Udemy_prac/retail_sales_dataset.csv" DBMS=CSV REPLACE;
run;

PROC PRINT DATA=work.sales (OBS=10);
RUN;

PROC CONTENTS DATA=work.sales;
RUN;

proc means data=work.sales;
var _numeric_;
run;

proc freq data=work.sales;
tables _character_;
run;


/** Transform Clean data in a new dataset**/

DATA work.cleaned_sales;
    SET work.sales;
    
    * Handle missing values - example: Set missing age to 41 (median for data), missing quantity/price to 0;
    IF MISSING(age) THEN age = 41;
    IF MISSING(quantity) THEN quantity = 0;
    IF MISSING(Price) THEN Price = 0;
   
    * Handle  missing date values;
	IF MISSING(date) THEN missing_date_flag = 1;
    ELSE missing_date_flag = 0;
    
    FORMAT date DATE9.; *Readable format;
    
    * Verify/Recalculate total_amount if inconsistent;
    calculated_total = quantity * Price;
    IF total_amount NE calculated_total THEN total_amount = calculated_total;  * Fix mismatches;
    
RUN;

* Sort and remove duplicates;
PROC SORT DATA=work.cleaned_sales NODUPKEY;
    BY _ALL_;  * Dedup on all variables;
RUN;

proc print data =work.cleaned_sales (obs=10);
run;

/* * ***********************/



**Create Dim_Date from unique dates;
PROC SQL;
    CREATE TABLE work.dim_date AS
    SELECT DISTINCT
        date AS date_key,  
        date,
        YEAR(date) AS year,
        MONTH(date) AS month,
        QTR(date) AS quarter,  
        DAY(date) AS day,
        WEEKDAY(date) AS day_of_week, 
        (WEEKDAY(date) IN (1,7)) AS is_weekend  /** 1=weekend, 0=weekday;*/
    FROM work.cleaned_sales
    WHERE NOT MISSING(date)
    ORDER BY date_key;
QUIT;

* Create other Dimension Tables (as before);


PROC SQL;
    CREATE TABLE work.dim_customers AS
    SELECT DISTINCT
        Customer_id,
        gender,
        age
    FROM work.cleaned_sales
    WHERE NOT MISSING(Customer_id);
    
    CREATE TABLE work.dim_products AS
    SELECT DISTINCT
        Product_Category AS product_key,
        Product_Category
    FROM work.cleaned_sales
    WHERE NOT MISSING(Product_Category);
QUIT;

* Create Fact Table with foreign keys, including date_key;

RUN;
PROC SQL;
    CREATE TABLE work.fact_sales AS
    SELECT
        transaction_id,
        date AS date_key,  /* FK to dim_date*/
        customer_id,   /* FK to dim_customers*/
        Product_Category AS product_key, /* FK to dim_products*/
        quantity,
        price,
        total_amount
    FROM work.cleaned_sales;
QUIT;

PROC PRINT DATA=work.fact_sales (OBS=10); RUN;


PROC EXPORT DATA=work.dim_date
    OUTFILE="/home/u61627519/Udemy_practice/Udemy_prac/dim_date.xlsx"
    DBMS=xlsx REPLACE;
    SHEET="dim_date";
    
PROC EXPORT DATA=work.dim_customers
    OUTFILE="/home/u61627519/Udemy_practice/Udemy_prac/dim_customers.xlsx"
    DBMS=xlsx REPLACE;
    SHEET="dim_customers";
    
PROC EXPORT DATA=work.dim_products
    OUTFILE="/home/u61627519/Udemy_practice/Udemy_prac/dim_products.xlsx"
    DBMS=xlsx REPLACE;
    SHEET="dim_products";
    
PROC EXPORT DATA=work.fact_sales
    OUTFILE="/home/u61627519/Udemy_practice/Udemy_prac/fact_sales.xlsx"
    DBMS=xlsx REPLACE;
    SHEET="fact_sales";
    
PROC EXPORT DATA=work.sales
    OUTFILE="/home/u61627519/Udemy_practice/Udemy_prac/sales.xlsx"
    DBMS=xlsx REPLACE;
    SHEET="sales";

%MEND;

%ETL_Project


/********************************************
*                                            *
*		 Output Profiling Report             *  
*                                            *
********************************************/
ODS HTML FILE="/home/u61627519/Udemy_practice/Udemy_prac/profiling_report.html";
TITLE 'Data Profiling Before and After ETL';

* Output Profiling Report;
ODS HTML FILE="/home/u61627519/Udemy_practice/Udemy_prac/profiling_report.html";


TITLE 'Data Profiling - Before ETL';

* Profile Raw Dat;
PROC CONTENTS DATA=work.sales; RUN;

PROC FREQ DATA=work.sales;
    TABLES gender product_category / MISSING;
RUN;

PROC MEANS DATA=work.sales N NMISS MIN MAX MEAN;
    VAR age quantity price total_amount;
RUN;

PROC UNIVARIATE DATA=work.sales;
    VAR total_amount;
RUN;



TITLE 'Data Profiling - After ETL';

/* Profile Cleaned/Fact Data (post-ETL, focus on new dim);*/
PROC FREQ DATA=work.fact_sales;
    TABLES product_key / MISSING;
RUN;

PROC MEANS DATA=work.fact_sales N NMISS MIN MAX MEAN;
    VAR quantity price total_amount;
RUN;

* Profile Dim_Date for Time Insights;
PROC FREQ DATA=work.dim_date;
    TABLES year month quarter day_of_week is_weekend;  * Distributions (e.g., % weekends);
RUN;

PROC MEANS DATA=work.dim_date N MIN MAX;
    VAR date_key;  * Date range (min/max);
RUN;


ODS HTML CLOSE;


