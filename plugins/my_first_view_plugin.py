from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint
from flask_appbuilder import expose, BaseView as AppBuilderBaseView

# define a Flask blueprint
my_blueprint = Blueprint(
    "test_plugin",
    __name__,
    # register airflow/plugins/templates as a Jinja template folder
    template_folder="templates",
)


class MyBaseView(AppBuilderBaseView):
    default_view = "test"

    @expose("/")
    def test(self):
        # render the HTML file from the templates directory with content
        return self.render_template("test.html", content="awesome")


# instantiate MyBaseView
my_view = MyBaseView()

# define the path to my_view in the Airflow UI
my_view_package = {
    # define the menu sub-item name
    "name": "Test View",
    # define the top-level menu item
    "category": "My Extra View",
    "view": my_view,
}


class MyViewPlugin(AirflowPlugin):
    # name the plugin
    name = "My app builder view"
    # add the blueprint and app builder_views components
    flask_blueprints = [my_blueprint]
    appbuilder_views = [my_view_package]
