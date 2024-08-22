INSERT INTO shopify_data
SELECT 
id
, shop_domain 
, application_id 
, autocomplete_enabled 
, user_created_at_least_one_qr 
, nbr_merchandised_queries 
, cast(nbrs_pinned_items as int[])
, showing_logo 
, has_changed_sort_orders 
, analytics_enabled 
, use_metafields 
, nbr_metafields 
, use_default_colors 
, show_products 
, instant_search_enabled 
, instant_search_enabled_on_collection 
, only_using_faceting_on_collection 
, use_merchandising_for_collection 
, index_prefix 
, indexing_paused 
, install_channel 
, cast(export_date as date)
, has_specific_prefix 


FROM temp_shopify_table_{formatted_date}
ON CONFLICT (id) DO UPDATE SET
shop_domain = EXCLUDED.shop_domain, 
application_id = EXCLUDED.application_id, 
autocomplete_enabled = EXCLUDED.autocomplete_enabled, 
user_created_at_least_one_qr = EXCLUDED.user_created_at_least_one_qr, 
nbr_merchandised_queries = EXCLUDED.nbr_merchandised_queries, 
nbrs_pinned_items = EXCLUDED.nbrs_pinned_items, 
showing_logo = EXCLUDED.showing_logo, 
has_changed_sort_orders = EXCLUDED.has_changed_sort_orders, 
analytics_enabled = EXCLUDED.analytics_enabled, 
use_metafields = EXCLUDED.use_metafields, 
nbr_metafields = EXCLUDED.nbr_metafields, 
use_default_colors = EXCLUDED.use_default_colors, 
show_products = EXCLUDED.show_products, 
instant_search_enabled = EXCLUDED.instant_search_enabled, 
instant_search_enabled_on_collection = EXCLUDED.instant_search_enabled_on_collection, 
only_using_faceting_on_collection = EXCLUDED.only_using_faceting_on_collection, 
use_merchandising_for_collection = EXCLUDED.use_merchandising_for_collection, 
index_prefix = EXCLUDED.index_prefix, 
indexing_paused = EXCLUDED.indexing_paused, 
install_channel = EXCLUDED.install_channel, 
export_date = EXCLUDED.export_date, 
has_specific_prefix = EXCLUDED.has_specific_prefix;