-- create client_ingests schema
create schema if not exists client_ingests;
-- create shopify data table
create table if not exists client_ingests.shopify_data (
    id varchar(256) PRIMARY KEY  not null
    , shop_domain varchar(256)
    , application_id varchar(256) not null
    , autocomplete_enabled boolean
    , user_created_at_least_one_qr boolean
    , nbr_merchandised_queries integer
    , nbrs_pinned_items integer[]
    , showing_logo boolean
    , has_changed_sort_orders boolean
    , analytics_enabled boolean
    , use_metafields boolean
    , nbr_metafields numeric
    , use_default_colors boolean
    , show_products boolean
    , instant_search_enabled boolean
    , instant_search_enabled_on_collection boolean
    , only_using_faceting_on_collection boolean
    , use_merchandising_for_collection boolean
    , index_prefix varchar(256)
    , indexing_paused boolean
    , install_channel varchar(256)
    , export_date date
    , has_specific_prefix boolean
    );