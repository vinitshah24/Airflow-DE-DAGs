from airflow.plugins_manager import AirflowPlugin

# creating a new top-level menu item
appbuilder_mitem_toplevel = {
    "name": "My Custom Apache Plugin",
    "href": "https://www.apache.org/",
}

# creating a new sub-item in the Docs menu item
appbuilder_mitem_subitem = {
    "name": "Astro SDK Docs",
    "href": "https://astro-sdk-python.readthedocs.io/en/stable/index.html",
    "category": "Docs",
}


class MyMenuItemsPlugin(AirflowPlugin):
    # defining the plugin class
    name = "Menu items plugin"

    # adding the menu items to the plugin
    appbuilder_menu_items = [appbuilder_mitem_toplevel, appbuilder_mitem_subitem]
