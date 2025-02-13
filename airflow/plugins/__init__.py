from airflow.plugins_manager import AirflowPlugin
from plugins.slack_notifications import send_failure_alert

class SlackNotificationPlugin(AirflowPlugin):
    name = 'slack_notification_plugin'
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
    appbuilder_views = []
    appbuilder_menu_items = []
    operators = []
    sensors = []