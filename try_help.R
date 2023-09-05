library(lubridate)
library(zoo)
install.packages("zoo")
install.packages("tidymodels")
library(tidymodels)
library(ggplot2)
install.packages("purr")

library(purr)

myconn <-dbConnect(dbDriver("PostgreSQL")
                   , read.xlsx("~/creds_n.xlsx", colNames = F)[5,2]
                   , port=read.xlsx("~/creds_n.xlsx", colNames = F)[6,2]
                   , dbname=read.xlsx("~/creds_n.xlsx", colNames = F)[2,2]
                   , user=read.xlsx("~/creds_n.xlsx", colNames = F)[3,2]
                   , password=read.xlsx("~/creds_n.xlsx", colNames = F)[4,2]
)

sql_script <- paste(scan("~/Elasticnet_cb.sql",
                         what="",sep="\n",blank.lines.skip=FALSE),collapse="\n")


tic()
# Run the SQL code
df <- dbGetQuery(myconn,
                 sql_script, stringsAsFactors=F)
toc()


# Disconnect from the Server
dbDisconnect(myconn)

myconn <-dbConnect(dbDriver("PostgreSQL")
                   , read.xlsx("~/creds_n.xlsx", colNames = F)[5,2]
                   , port=read.xlsx("~/creds_n.xlsx", colNames = F)[6,2]
                   , dbname=read.xlsx("~/creds_n.xlsx", colNames = F)[2,2]
                   , user=read.xlsx("~/creds_n.xlsx", colNames = F)[3,2]
                   , password=read.xlsx("~/creds_n.xlsx", colNames = F)[4,2]
)


cal_wk <- c("SELECT distinct fisc_wk_id, fisc_wk_strt_dt, fisc_qtr_strt_dt
FROM edwp.cal_day_dim ")



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
  
  sales_recipe <- recipe(log_units ~ log_price + month + week_of_month + year, data = data) %>%
  ###sales_recipe <- recipe(log_units ~ log_price , data = data) %>%
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

price_elasticities_tidymodels_data <- scope_data %>% 
  mutate(season = case_when( month %in% c(3,4,5) ~ "spring",
                          month %in% c(6,7,8) ~ "Summer",
                          month %in% c(9,10,11) ~ "Fall",
                          TRUE ~ "Winter",
                           )) %>% 
  filter( 
    season == "spring") %>%
  group_by(mkt_lvl) %>% 
    nest() %>%
  
  mutate(elasticities = purrr::map(data, fit_elasticnet_tidymodels)) 

price_elasticities_tidymodels = price_elasticities_tidymodels_spring %>% 
  unnest_longer( elasticities) %>%
  select(mkt_lvl, elasticities, elasticities_id) %>%
  pivot_wider(names_from = elasticities_id , values_from = elasticities,values_fill  = 0)


##summer
price_elasticities_tidymodels_summer <- scope_data %>% 
  mutate(season = case_when( month %in% c(3,4,5) ~ "spring",
                             month %in% c(6,7,8) ~ "summer",
                             month %in% c(9,10,11) ~ "fall",
                             TRUE ~ "winter",
  )) %>% 
  filter( 
    season == "summer") %>%
  group_by(mkt_lvl) %>% 
  nest() %>%
  
  mutate(elasticities = purrr::map(data, fit_elasticnet_tidymodels)) 

price_elasticities_tidymodels_summer = price_elasticities_tidymodels_summer %>% 
  unnest_longer( elasticities) %>%
  select(mkt_lvl, elasticities, elasticities_id) %>%
  pivot_wider(names_from = elasticities_id , values_from = elasticities,values_fill  = 0)

##fall
price_elasticities_tidymodels_fall <- scope_data %>% 
  mutate(season = case_when( month %in% c(3,4,5) ~ "spring",
                             month %in% c(6,7,8) ~ "summer",
                             month %in% c(9,10,11) ~ "fall",
                             TRUE ~ "winter",
  )) %>% 
  filter( 
    season == "fall") %>%
  group_by(mkt_lvl) %>% 
  nest() %>%
  
  mutate(elasticities = purrr::map(data, fit_elasticnet_tidymodels)) 

price_elasticities_tidymodels_fall = price_elasticities_tidymodels_fall %>% 
  unnest_longer( elasticities) %>%
  select(mkt_lvl, elasticities, elasticities_id) %>%
  pivot_wider(names_from = elasticities_id , values_from = elasticities,values_fill  = 0)


##winter
price_elasticities_tidymodels_winter <- scope_data %>% 
  mutate(season = case_when( month %in% c(3,4,5) ~ "spring",
                             month %in% c(6,7,8) ~ "summer",
                             month %in% c(9,10,11) ~ "fall",
                             TRUE ~ "winter",
  )) %>% 
  filter( 
    season == "winter") %>%
  group_by(mkt_lvl) %>% 
  nest() %>%
  
  mutate(elasticities = purrr::map(data, fit_elasticnet_tidymodels)) 

price_elasticities_tidymodels_winter = price_elasticities_tidymodels_winter %>% 
  unnest_longer( elasticities) %>%
  select(mkt_lvl, elasticities, elasticities_id) %>%
  pivot_wider(names_from = elasticities_id , values_from = elasticities,values_fill  = 0)


# extract the market and elasticity columns from the four data sets
mkt_lvl <- price_elasticities_tidymodels$mkt_lvl
elasticity_spring <- price_elasticities_tidymodels$price_elasticity
elasticity_summer <- price_elasticities_tidymodels_summer$price_elasticity
elasticity_fall <- price_elasticities_tidymodels_fall$price_elasticity
elasticity_winter <- price_elasticities_tidymodels_winter$price_elasticity

# combine the market and elasticity columns using cbind()
combined_data <- cbind(mkt_lvl, elasticity_spring, elasticity_summer, elasticity_fall, elasticity_winter)

# view the combined data set
head(combined_data) 

