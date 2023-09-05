library(lubridate)
library(zoo)
install.packages("zoo")
install.packages("tidymodels")
library(tidymodels)
library(ggplot2)
install.packages("purr")

library(purr)



sql_script <- paste(scan("~/elastic_dfsql",
                         what="",sep="\n",blank.lines.skip=FALSE),collapse="\n")



df <- dbGetQuery(myconn,
                 sql_script, stringsAsFactors=F)



# Disconnect from the Server
dbDisconnect(myconn)



cal_wk <- c("SELECT distinct fisc_wk_id, fisc_wk_strt_dt, fisc_qtr_strt_dt
")



sql_script <- paste(cal_wk,collapse="\n")



# Run the SQL code
cal_wk <- dbGetQuery(myconn,
                     sql_script, stringsAsFactors=F)



# Disconnect from the Server
dbDisconnect(myconn)

scope_data1 <- df%>% 
  left_join (cal_wk) 



data <- read_xlsx(paste0(home_folder,'chicken_db.xlsx'))
data <- na.omit(data)


# Convert "month" column to date format
data$month <- as.yearmon(data$month, "%Y %b")

# Convert date format to "yyyymm" format
data$month <- format(data$month, "%Y%m")

# Create a new column "date" by concatenating "year" and "month"
###data$date <- paste(data$month, "01", sep = "-")

# Convert "date" column to Date format
##data$date <- as.Date(paste0(substr(data$date, 1, 6), "01"), "%Y%m%d")

data <- data %>% rename(cal_mo_id = month) %>% 
  mutate(cal_mo_id = as.integer(cal_mo_id))


scope_data <- scope_data1 %>%
  left_join(data %>% select(cal_mo_id, value)) %>% 
  mutate(
    month = month(fisc_wk_strt_dt),
    year = year(fisc_wk_strt_dt),
    week_of_month = week(fisc_wk_strt_dt))%>%
  group_by(fisc_wk_strt_dt,month, year, week_of_month, mkt_lvl,value) %>%
  summarize(total_wgt = sum(total_wgt, na.rm = T),
            total_sales = sum(total_sales, na.rm = T),
            avg_unit_price = total_sales/total_wgt) %>% 
  mutate(avg_unit_price_adj = (avg_unit_price/value)*100 ,
         log_price = log(avg_unit_price_adj),
         log_units = log(total_wgt)) %>%
  
  ungroup()



fit_elasticnet_tidymodels  <- function(data) {
  
  
  
  
  elasticnet_spec <- linear_reg(penalty = tune(), mixture = 0.5) %>%
    set_engine("glmnet") %>%
    set_mode("regression")
  
  ###sales_recipe <- recipe(log_units ~ log_price +  month + week_of_month + year, data = data) %>%
  sales_recipe <- recipe(log_units ~ log_price , data = data) %>%
    step_dummy(all_nominal_predictors(), -all_outcomes()) %>%
    step_normalize(all_numeric_predictors(), -all_outcomes()) %>% 
    
    step_zv(all_predictors()) 
  
  sales_recipe_prep <- prep(sales_recipe)
  sales_recipe_juiced <- juice(sales_recipe_prep)
  
  
  
  
  # Create a 10-fold cross-validation object
  sales_vfold <- vfold_cv(data, v = 10, strata = log_units)
  # Grid search for tuning the penalty parameter
  grid_vals <- tibble(penalty = 10^seq(-6, -1, length.out = 20))
  
  
  
  # Perform cross-validated grid search
  elasticnet_tune <- tune_grid(elasticnet_spec,
                               preprocessor  = sales_recipe,
                               resamples = sales_vfold,
                               grid = grid_vals,
                               metrics = metric_set(rmse, rsq))
  
  
  
  
  # Extract the best model
  best_model <- elasticnet_tune %>%
    select_best(metric = "rmse")
  
  
  
  
  # Prepare the recipe for the whole dataset
  prepared_recipe <- prep(sales_recipe, training = data)
  baked_data <- bake(prepared_recipe, new_data = data)
  
  
  # Fit the best model on the whole dataset
  best_fit <- elasticnet_spec %>%
    finalize_model(best_model) %>%
    fit(log_units ~ ., data = baked_data)
  
  
  
  # Extract the log_price and log_units_processed coefficient
  
  coefficients <-  tidy(best_fit)
  recipe_tidied <- tidy(sales_recipe)
  
  coefs_normalized <- coefficients %>%
    filter(term %in% c('log_price','log_units_processed')) %>%
    select(-penalty)
  
  
  
  normalized_stats <- tidy(prepared_recipe, number = 2) %>%
    filter(terms %in% c('log_price')) %>%
    filter(statistic == 'sd') %>% select(-statistic) %>% 
    rename(sd = value, term = terms) %>%
    select(-id)
  
  
  coefs_denormalized = coefs_normalized  %>% inner_join(normalized_stats) %>%
    mutate(beta_denormalized = estimate / sd)
  
  log_price_beta = coefs_denormalized %>% filter(term == 'log_price') %>% pull(beta_denormalized)
  
  
  
  result <- list('price_elasticity' = log_price_beta )
  
  
  return(result) }



scope_data <- na.omit(scope_data)

price_elasticities_tidymodels3 <- scope_data %>% 
  filter( 
    fisc_wk_strt_dt > "2022-05-01") %>%
  group_by(mkt_lvl) %>% nest() %>%
  
  mutate(elasticities = purrr::map(data, fit_elasticnet_tidymodels)) 


