from great_tables import GT
from great_tables.data import gtcars
import gt_extras as gte

gtcars_mini = gtcars.iloc[5:15].copy().reset_index(drop = True)

gtcars_mini["efficiency"] = gtcars_mini["mpg_c"] / gtcars_mini["hp"] * 100

(
    GT(gtcars_mini, rowname_col = "model")
    .tab_stubhead(label = "Vehicle")
    .cols_hide(["drivetrain","hp_rpm", "trq_rmp","trim","bdy_style","msrp","trsmn","ctry_origin"])
    .cols_align("center")
    .tab_header(title = "Car Performance Review", subtitle="Using gt-extras funct")

    # add gt-extras features using gt.pipe()
    .pipe(gte.gt_color_box, columns = ["hp","trq"], palette =["red", "green"])
    .pipe(gte.gt_plt_dot, category_col = "mfr", data_col = "efficiency", domain =[0,0])
    .pipe(gte.gt_plt_bar, columns = ["mpg_c", "mpg_h"])
    .pipe(gte.gt_fa_rating, columns = "efficiency")
    .pipe(gte.gt_hulk_col_numeric, columns = "year", palette = "viridis")
    .pipe(gte.gt_theme_538)
)

