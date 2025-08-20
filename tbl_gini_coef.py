
# import packages
import polars as pl

# data and setup
pre_tax_col = "gini_market__age_total"
post_tax_col = "gini_disposable__age_total"

# read the data
df = pl.read_csv(
    "https://raw.githubusercontent.com/rfordatascience/tidytuesday/main/data/2025/2025-08-05/income_inequality_raw.csv",
    schema={
        "Entity": pl.String,
        "Code": pl.String,
        "Year": pl.Int64,
        post_tax_col: pl.Float64,
        pre_tax_col: pl.Float64,
        "population_historical": pl.Int64,
        "owid_region": pl.String,
    },
    null_values=["NA", ""],
)

df = (
    df.sort("Entity")
    .group_by("Entity", maintain_order=True)
    .agg(
        [
            pl.col("Code"),
            pl.col("Year"),
            pl.col(post_tax_col),
            pl.col(pre_tax_col),
            pl.col("population_historical"),
            pl.col("owid_region").fill_null(strategy="backward"),

        ]
    )
    .explode(
        [
            "Code",
            "Year",
            post_tax_col,
            pre_tax_col,
            "population_historical",
            "owid_region",
        ]
    )
)


# drop nulls 
df = df.drop_nulls(
    subset=(
        pl.col(post_tax_col),
        pl.col(pre_tax_col),
    )
)


# compute percentage reduction in gini coefficient
df = df.with_columns(
    ((pl.col(pre_tax_col)-pl.col(post_tax_col))/pl.col(pre_tax_col)*100)
    .round(2)
    .alias("gini_pct_change")   
)

df.head(2)

# calculate five year benchmark mean or pct chagen for each country
df  = df.with_columns(
    pl.col("gini_pct_change")
    .rolling_mean(window_size=5)
    .over(pl.col("Entity"))
    .alias("gini_pct_benchmark_5yr")
)

# select rows with large population in 2020, sorted by coef post tax
df = (
    # choose smaller pop to include more cointries
    df.filter(pl.col("population_historical").gt(40000000))
    .filter(pl.col("Year").eq(2020))
    .sort(by=pl.col(post_tax_col))
)


# scale population
df = df.with_columns((pl.col("population_historical").log10()).alias("pop_log"))
pop_min = df["pop_log"].min()/1
pop_max = df["pop_log"].max()


# set up gt-extras icons scaling populatio to 1-10 range
df = df.with_columns(
    ((pl.col("pop_log")- pop_min)/(pop_max-pop_min)*10+1)
    .round(0)
    .cast(pl.Int64)
    .alias("pop_icons")
)

# format original population values with commas
df = df.with_columns(
    pl.col("population_historical").map_elements(
        lambda x: f"{int(x):,}" if x is not None else None, return_dtype=pl.String
    )
)

df.head(2)


from great_tables import GT, html
import gt_extras as gte

# apply gte.fa_icon_repeat to each entry n the pop_icon column
df_with_icons = df.with_columns(
    pl.col("pop_icons").map_elements(
        lambda x: gte.fa_icon_repeat(name = "person", repeats=int(x)),
        return_dtype = pl.String,
    )
)

# generate the table before gte extras addons
gt = (
    GT(df_with_icons, rowname_col="Entity", groupname_col = "owid_region")
    .tab_header(
        "Income Inequality Before and After Taxes in 2020",
        "As measured by the Gini coefficient, where 0 is best and 1 is worst",
    )
    .cols_move("pop_icons", after=pre_tax_col)
    .cols_align("left")
    .cols_hide(["Year","pop_log","population_historical"])
    .fmt_flag("Code")
    .cols_label(
        {
            "Code": "",
            "gini_pct_change": "Improvement Post Taxes",
            "pop_icons": "Population",
        }
    )
    .tab_source_note(
        html(
            """
            <div>
            <strong>Source:</strong> Data from <a href="https://github.com/rfordatascience/tidytuesday">#TidyTuesday</a> (2025-08-05).<br>
                <div>
                <strong>Dumbbell plot:</strong>
                <span style="color:#DE3163;">Red:</span> Pre-tax Gini Coefficient
                <span style="color:#1abc9c;">Green:</span> Pos-tax Gini coefficient
                <br>
                </div>
            <strong>Bullet plot:</strong> Percent reduction in Gini after taxes for each country, compared to its 5-year average benchmark.
            </div>
            """
        )
    )
)


# apply the gt-extras function via pipe
(
    gt.pipe(
        gte.gt_plt_dumbbell,
        col1=pre_tax_col,
        col2=post_tax_col,
        col1_color = "#DE3163",
        col2_color = "#1abc9c",
        dot_border_color = "transparent",
        num_decimals=2,
        width =240,
        label = "Pre-tax to Post-tax Coefficient"
    )
    .pipe(
        gte.gt_plt_bullet,
        "gini_pct_change",
        "gini_pct_benchmark_5yr",
        fill="#1abc9c",
        target_color = "#000040",
        bar_height = 13,
        width = 200,
    )
    .pipe(
        gte.gt_merge_stack,
        col1="pop_icons",
        col2="population_historical",
    )
    .pipe(gte.gt_theme_guardian)
)


