# -*- coding: utf-8 -*-
"""
PULL类告警源适配器DEMO, 在Demo代码的基础上主要需要完善的是拉取告警的方法(pull_alarm), 以及清洗告警字段的方法(clean_xx)
"""

import json
import math
import requests
import hashlib
from datetime import datetime
# 已引用模块请不要删除
# from django.utils import timezone
# from django.db.models import F
# from alarm_collect.models import AlarmSource


from alarm.models import AlarmEvent
from alarm_collect.clients.custom.baseclient import BaseClient
from common.log import logger
from alarm_collect.clients.custom.baseclient import AlarmClientError, AlarmSourceConfigsDepletion, PullAlarmError
from alarm_collect.models import AlarmSource

# 告警等级映射, 请根据实际告警源情况, 修改ALARM_LEVEL_MAPPING的值
# 其中ALARM_LEVEL_MAPPING的key对应的是告警源数据中的等级，value为告警中心中的实际等级
# 统一告警中心v2.6及以上版本，支持自定义告警等级, 系统设置中已添加的告警等级才能在下方使用
# 如：源数据中等级为"level1", 需要清洗成告警中对应的自定义等级的"warning1"
# 则：ALARM_LEVEL_MAPPING = { "level1": "warning1"}
# 当前系统中已有告警等级名称及对应英文名如下：
# 等级中文名 : 等级英文名
# 提醒 : remind
# 预警 : warning
# 致命 : fatal
# 一般 : general
# 重要 : important

ALARM_LEVEL_MAPPING = {
    1: "remind",
    2: "general",
    3: "warning",
    4: "critical",
    5: "fatal"
}


class AlarmClient(BaseClient):
    """
    PULL类告警源适配器Client

    ALARM_BASE_KEY_DICT: 告警事件字段, 不可修改预定义字段的标识名和显示名
    如果需要更多的告警字段, 在ALARM_BASE_KEY_DICT中自定义添加拓展字段，格式请参照预定义字段，
    并设置对应显示名, 且在告警源适配器中添加对应字段清洗方法 clean_xx (xx为扩展字段)
    注意：不同的告警源使用同一个拓展字段时，字段的标识名和中文显示名必须一一对应，不可在不同告警源将同一个标识名的扩展字段设置为不同的显示名。

    当返回内容需要包含url转跳可以在内容中直接返回html标签内容（除clean_level外均支持），参照下方clean_alarm_content方法

    告警字段清洗说明, 下列中某些字段(如id, source_type, bk_biz_name等)未给出清洗方法, 是已在BaseClient具备默认清洗规则,

    非必要情况下无需重写清洗方法, 如需丰富告警CMDB相关字段, 请重写相关清洗方法(如:　clean_bk_biz_id，　clean_bk_biz_name等）
    """
    ALARM_BASE_KEY_DICT = dict(  # noqa
        [
            ("alarm_id", "告警ID"),
            ("source_id", "告警源ID"),
            ("source_name", "告警源名称"),
            ("item", "告警指标"),
            ("name", "告警名称"),
            ("event_id", "告警事件ID"),
            ("alarm_time", "告警时间"),
            ("content", "告警内容"),
            ("action", "告警动作"),
            ("level", "告警等级"),
            ("object", "告警对象"),
            ("bk_biz_id", "CMDB业务ID"),
            ("bk_biz_name", "CMDB业务名称"),
            ("bk_set_id", "CMDB集群ID"),
            ("bk_set_name", "CMDB集群名称"),
            ("bk_module_id", "CMDB模块ID"),
            ("bk_module_name", "CMDB模块名称"),
            ("bk_cloud_id", "CMDB云区域ID"),
            ("bk_obj_id", "CMDB模型ID"),  # 告警事件CMDB拓扑图需要字段, 需要拓扑图功能, 在适配器中补充获取方法
            ("bk_inst_id", "CMDB模型实例ID"),  # 告警事件CMDB拓扑图需要字段, 在适配器中补充获取方法
            ("meta_info", "告警元信息"),
            ("bk_service_id", "CMDB服务实例ID"),
            # ("alarm_close_msg", "告警关闭信息"),  # 告警事件被告警源关闭或恢复时携带的信息字段,可以自定义数量、标识名和中文名
            # ("alarm_resolve_msg", "告警关闭信息"),  # 注意需要定义对应字段的清洗方法和get_custom_msg_fields方法
        ]
    )

    # 定义告警源拉取告警是否需要额外配置信息
    # 说明: 页面定义的配置, 直接在代码中使用 config_key = self.alarm_source_config_dict.get("config_key") 即可获取到
    # 举例: host = self.alarm_source_config_dict.get("host", "")
    ALARM_SOURCE_NEED_CONFIGS = True

    def __init__(self, alarm_source_obj):
        super(AlarmClient, self).__init__(alarm_source_obj)

    def pull_alarm(self):
        """拉取告警事件, 需要的源适配器自行实现"""
        params = {"status": 10}
        # 告警源的meta_info可以存储上一次拉取告警时间
        if self.alarm_source_obj.meta_info:
            params["start_time"] = self.alarm_source_obj.meta_info

        self.alarm_list = self.query_alarm(self.alarm_source_config_dict, params)

    @classmethod
    def query_alarm(cls, config, params):
        access_token = cls.get_access_token(config)
        host = config["host"].rstrip("/")
        api = f"{host}/alarms/"
        headers = {"Authorization": f"Bearer {access_token}"}

        PAGE_SIZE = 500
        params.update(page=1, size=1)
        data = cls.__query_alarm(api, params, headers)
        page_count = math.ceil(data["count"] / PAGE_SIZE)
        alarm_list = []
        for i in range(page_count):
            params.update(page=i + 1, size=PAGE_SIZE)
            data = cls.__query_alarm(api, params, headers)
            alarm_list.extend(data["results"])

        return alarm_list

    @staticmethod
    def __query_alarm(api, params, headers):
        try:
            resp = requests.get(api, params=params, headers=headers, verify=False)
            data = resp.json()
        except json.decoder.JSONDecodeError:
            logger.error(f"pull_alarm failed, status: {resp.status_code}, response: {resp.text}")
            raise PullAlarmError("拉取告警失败")
        except Exception as e:
            logger.exception(f"request [GET]{api} failed, params: {params}")
            raise
        logger.info(f"pull_alarm succeed, response data: {data}")
        return data

    @staticmethod
    def get_access_token(config):
        try:
            host = config["host"].rstrip("/")
            username = config["username"]
            password = config["password"]
        except KeyError:
            raise AlarmSourceConfigsDepletion("插件参数未填写")
        api = f"{host}/api/token/"
        params = {
            "username": username,
            "password": password
        }
        resp = requests.post(api, json=params, verify=False)
        try:
            data = resp.json()
        except json.decoder.JSONDecodeError:
            logger.error(f"get_token failed, status: {resp.status_code}, response: {resp.text}")
            raise AlarmClientError("token获取失败")
        logger.info(f"get_token succeed, response data: {data}")
        return data.get("access", "")

        # 当告警没有唯一ID，需要查询库中所有告警数据进行对比时可以使用下面方法：
        # 根据event_id查询对应的告警是否存在（此处event_id为clean_event_id方法的结果）：
        # alarms = self.query_alarm_source_all_alarm_by_event_id(event_ids)
        # event_ids 样例： ["event_id1", "event_id2"]
        # type(alarms) == List(Dict)

        # 恢复告警：
        # self.match_alarm_resolved(alarms)
        # alarms为上方返回的告警列表结果中需要恢复的告警组成的列表

    @staticmethod
    def clean_item(alarm):
        """清洗告警指标, 请根据alarm中实际字段更改清洗方法, 必须字段"""
        return alarm.get("alarm_item", "")

    @staticmethod
    def clean_name(alarm):
        """清洗告警名称, 请根据alarm中实际字段更改清洗方法, 必须字段"""
        return alarm.get("alarm_title", "")

    def clean_event_id(self, alarm):
        """
        清洗告警事件ID, event_id用于在告警中心中标识告警是否跟之前已获取到的告警为同一条告警，
        各告警源可以自定义生成规则。此字段跟action字段组合来表示告警的产生/恢复。
        请根据alarm中实际字段更改清洗方法, 必须字段
        """
        return hashlib.md5("{}:{}".format(self.alarm_source_id, alarm["id"]).encode("utf8")).hexdigest()

    @staticmethod
    def clean_alarm_time(alarm):
        """清洗告警时间, 请根据alarm中实际字段更改清洗方法, 必须字段 仅支持时间字符串，格式为： %Y-%m-%d %H:%M:%S"""
        return datetime.fromtimestamp(float(alarm["alarm_time"]) / 1000).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def clean_content(alarm):
        """清洗告警内容, 请根据alarm中实际字段更改清洗方法, 必须字段"""
        # 返回包含url转跳的内容示例如下：
        # return '测试内容<a href="http://www.test.com" target="_blank">test</a>'
        return alarm.get("alarm_content", "")

    @staticmethod
    def clean_action(alarm):
        """
        清洗告警动作, 请根据alarm中实际字段更改清洗方法, 必须字段
        firing代表产生新的告警, resolved代表消除同event_id告警
        """
        ALARM_ACTION_MAPPING = {
            10: "firing",
            20: "close",
            30: "resolved"
        }
        return ALARM_ACTION_MAPPING.get(alarm["status"])

    @staticmethod
    def clean_level(alarm):
        """
        清洗告警等级, 请根据alarm中实际字段更改清洗方法, 必须字段
        注意更新ALARM_LEVEL_MAPPING中的映射值, 用来把原告警等级清洗为告警中心的等级
        """
        return ALARM_LEVEL_MAPPING.get(alarm["alarm_level"])

    @staticmethod
    def clean_object(alarm):
        """清洗告警对象, 请根据alarm中实际字段更改清洗方法, 必须字段"""
        return alarm["alarm_object"]

    @staticmethod
    def clean_bk_obj_id(alarm):
        """
        清洗CMDB模型, 请根据alarm中实际字段更改清洗方法
        告警事件CMDB拓扑需要字段, 有此字段才支持拓扑图绘制
        """
        return alarm.get("bk_obj_id", "")

    @staticmethod
    def clean_bk_inst_id(alarm):
        """
        清洗CMDB模型实例, 请根据alarm中实际字段更改清洗方法
        告警事件CMDB拓扑需要字段, 有此字段才支持拓扑图绘制
        """
        return alarm.get("bk_inst_id", "")

    @staticmethod
    def clean_meta_info(alarm):
        """清洗告警元信息, 非必须字段, 可不更改代码"""
        return alarm.get("id", "")

    def get_metric(self, alarm, start_time, end_time):
        # """ 获取告警某个时间范围内的告警指标数据 """
        # alarm为清洗后完整的告警信息，包含所有告警源清洗出来的字段
        # type(start_time) == type(end_time) == type(datetime.datetime.strptime(time_str))
        # item = alarm.get("item") #告警指标中文名，如CPU空闲率
        # 处理逻辑 请放这里
        data = {
            "points": [ # 图表数据点列表
                {
                    "time": 1627304040000,  # 13位毫秒级时间戳
                    "value": 87.32          # 数据点的值，必须为float/int数值
                },
                {
                    "time": 1627309200000,
                    "value": 87.89
                },
                {
                    "time": 1627314360000,
                    "value": 88.07
                },
                {
                    "time": 1627319520000,
                    "value": 87.93
                }
            ],
            "name": "CPU使用率", # 图表标题(一般为当前指标名，用alarm["item"]即可）
            "unit": "%" # 数据单位
        }
        return data

    # @staticmethod
    # def clean_alarm_close_msg(alarm):
    #     """ 清洗在告警被恢复/关闭时额外携带的信息1 """
    #     # 从原始告警对象中获取自定义信息
    #     return alarm.get("alarm_close_msg_field", "")
    #
    # @staticmethod
    # def clean_alarm_resolve_msg(alarm):
    #     """ 清洗在告警被恢复/关闭时额外携带的信息2 """
    #     # 从原始告警对象中获取自定义信息
    #     return alarm.get("alarm_resolve_msg_field", "")

    #     @staticmethod
    #     def get_custom_msg_fields():
    #     """
    #         定义需要在告警恢复/关闭时新增或更新的字段列表（如恢复告警时新增告警恢复原因字段/更新告警内容字段）
    #         注意：返回的字段必须有对应的clean_xxx清洗方法且在ALARM_BASE_KEY_DICT中有定义
    #         @return: 需要处理的告警字段列表（如["resolve_reason", "content"]）
    #     """
    #         # 返回ALARM_BASE_KEY_DICT中自定义信息的标识名列表
    #         return ["alarm_close_msg", "alarm_resolve_msg"]

    def alarm_source_update(self):
        """告警源相关信息更新
        在父类中实现了更新告警源最后获取告警时间, 同步&接收产生告警数
        # 只记录产生类型的告警数量，保证存ES告警数量和页面显示数量能够对应
        firing_count = len([alarm for alarm in self.alarm_list if alarm["_clean_info"].get("action", "") == "firing"])
        # 更新告警源最后获取告警时间, 同步&接收告警次数, 同步告警最后ID
        AlarmSource.objects.filter(id=self.alarm_source_id).update(
            activate_time=timezone.now(), alarm_num=F("alarm_num") + firing_count
        )

        如果要记录某个标识用于下次调用可以参照下方注释内容，把标识存储在meta_info字段中，可以通过self.alarm_source_obj.meta_info获取
        at_last_alarm = self.alarm_list[-1]
        firing_count = len([alarm for alarm in self.alarm_list if alarm["_clean_info"].get("action", "") == "firing"])
        AlarmSource.objects.filter(id=self.alarm_source_id).update(
            alarm_num=F("alarm_num") + firing_count, meta_info=at_last_alarm.get("eventID", "")
        )
        """
        super(AlarmClient, self).alarm_source_update()
        AlarmSource.objects.filter(id=self.alarm_source_id).update(meta_info=int(datetime.now().timestamp() * 1000))

    @classmethod
    def alarm_resolved(cls, alarm_source_obj):
        """告警恢复方法
        当在插件页面配置了支持定时同步告警恢复状态
        系统会按照页面设置的间隔定期调用本方法，对告警进行恢复/关闭告警操作
        若没有配置则无需修改本方法"""
        # 恢复告警逻辑
        # 获取系统中当前告警源的活动告警
        # active_alarms = cls.query_alarm_source_not_close_alarm(alarm_source_obj)
        # 判断需要恢复告警的业务逻辑
        # recovered_alarm_list = []
        # close_alarm_list = []
        # for alarm in active_alarms:
        # 业务逻辑判断操作 if xxx:
        # recovered_alarm_list.append(alarm)
        # 业务逻辑判断操作 if xxx:
        # close_alarm_list.append(alarm)
        # 要恢复告警：(下方的recovered_alarm_list为active_alarms中需要恢复的告警字典组成的列表)
        # cls.match_alarm_resolved(recovered_alarm_list)
        # 要关闭告警：(下方的close_alarm_list为active_alarms中需要关闭的告警字典组成的列表)
        # cls.match_alarm_close(close_alarm_list)
        active_alarms = cls.query_alarm_source_not_close_alarm(alarm_source_obj)
        origin_id_alarm_mapping = {int(alarm["meta_info"]): alarm for alarm in active_alarms if alarm["meta_info"]}
        origin_ids = list(origin_id_alarm_mapping)

        config = alarm_source_obj.get_config_dict()
        params = {
            "status": 30,
            "alarm_ids": json.dumps(origin_ids[:400])
        }
        resolved_origin_alarms = cls.query_alarm(config, params)
        resolved_alarms = []
        for origin_alarm in resolved_origin_alarms:
            alarm = origin_id_alarm_mapping.get(origin_alarm["id"])
            if alarm:
                resolved_alarms.append(alarm)

        logger.info(f"{len(resolved_alarms)} alarm_resolved: {resolved_alarms}")
        cls.match_alarm_resolved(resolved_alarms)

