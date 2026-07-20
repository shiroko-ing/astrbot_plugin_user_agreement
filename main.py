import json

from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star
from astrbot.api import logger, AstrBotConfig
from astrbot.api.message_components import Node, Plain


class UserAgreement(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self._consented_users: set[str] = set()
        self._reminded_users: set[str] = set()
        self._load_consented_users()

    def _data_path(self):
        from pathlib import Path
        from astrbot.core.utils.astrbot_path import get_astrbot_data_path
        return Path(get_astrbot_data_path()) / "plugin_data" / self.name

    def _consent_file(self):
        return self._data_path() / "consented_users.json"

    def _load_consented_users(self):
        cf = self._consent_file()
        if cf.exists():
            try:
                data = json.loads(cf.read_text(encoding="utf-8"))
                self._consented_users = set(data)
                logger.info(f"[UserAgreement] 已加载 {len(self._consented_users)} 个已同意用户。")
            except Exception as e:
                logger.error(f"[UserAgreement] 加载同意列表失败: {e}")
                self._consented_users = set()

    def _save_consented_users(self):
        self._data_path().mkdir(parents=True, exist_ok=True)
        self._consent_file().write_text(
            json.dumps(list(self._consented_users), ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

    def _is_admin(self, sender_id: str) -> bool:
        admin_raw = self.config.get("admin_list", "")
        if not admin_raw:
            return False
        admins = [a.strip() for a in admin_raw.strip().split("\n") if a.strip()]
        return sender_id in admins

    def _cmd(self, name: str) -> str:
        return self.config.get(name, "")

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_all_message(self, event: AstrMessageEvent, *args):
        me = self if self is not None else event
        evt = args[0] if args else event

        sender_id = str(evt.message_obj.sender.user_id)
        message_str = evt.message_str.strip()

        if not message_str:
            return

        agree_cmd = me._cmd("agree_command")
        revoke_cmd = me._cmd("revoke_command")

        is_agree = message_str in ("/同意", "/agree", f"/{agree_cmd}", agree_cmd)
        is_revoke = revoke_cmd and message_str in (
            "/撤销所有同意", f"/{revoke_cmd}", revoke_cmd,
        )

        if is_agree:
            if sender_id in me._consented_users:
                yield evt.plain_result(me._cmd("already_agreed_text"))
            else:
                me._consented_users.add(sender_id)
                me._save_consented_users()
                logger.info(f"[UserAgreement] 用户 {sender_id} 已同意。")
                yield evt.plain_result(me._cmd("agree_success_text"))
            evt.stop_event()
            return

        if is_revoke:
            if me._is_admin(sender_id):
                count = len(me._consented_users)
                me._consented_users.clear()
                me._reminded_users.clear()
                me._save_consented_users()
                logger.info(f"[UserAgreement] 管理员 {sender_id} 撤销了所有 {count} 个同意。")
                yield evt.plain_result(me._cmd("revoke_success_text"))
            else:
                yield evt.plain_result(me._cmd("revoke_no_admin_text"))
            evt.stop_event()
            return

        if sender_id in me._consented_users:
            return

        if sender_id in me._reminded_users:
            yield evt.plain_result(me._cmd("repeat_reminder_text"))
            evt.stop_event()
            return

        me._reminded_users.add(sender_id)

        ua_title = me._cmd("user_agreement_title")
        pp_title = me._cmd("privacy_policy_title")

        ua_text = "\n".join(
            line.strip() for line in me._cmd("user_agreement").strip().split("\n")
            if line.strip()
        ) or "暂无"

        pp_text = "\n".join(
            line.strip() for line in me._cmd("privacy_policy").strip().split("\n")
            if line.strip()
        ) or "暂无"

        node_ua = Node(uin=sender_id, name=ua_title, content=[Plain(ua_text)])
        node_pp = Node(uin=sender_id, name=pp_title, content=[Plain(pp_text)])

        yield evt.plain_result(me._cmd("reminder_text"))
        yield evt.chain_result([node_ua, node_pp])
        evt.stop_event()
