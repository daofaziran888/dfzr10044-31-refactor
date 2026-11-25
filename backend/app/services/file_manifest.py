"""兼容别名：文件清单发布入口。"""
from app.services.manifest_publisher import *  # noqa: F401,F403
from app.services.manifest_publisher import publish_file_list as publish_file_manifest  # noqa: F401
from app.services.manifest_publisher import publish_manifest  # noqa: F401
