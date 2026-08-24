import json
import asyncio
from pathlib import Path
from typing import Set, Optional, Dict

from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star
from astrbot.api import logger, AstrBotConfig
from astrbot.api.message_components import Plain, Node

try:
    import aiosqlite
    HAS_AIOSQLITE = True
except ImportError:
    HAS_AIOSQLITE = False

class UserAgreementPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        
        # 数据存储
        self._data_dir = Path("data/plugin_data/astrbot_plugin_user_agreement")
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._db_path = self._data_dir / "consents.db"
        self._json_backup = self._data_dir / "consented_users.json"
        
        # 内存数据
        self._consented_users: Set[str] = set()
        self._reminded_users: Set[str] = set()
        self._qq_groups: Dict[str, Set[str]] = {}
        
        # 异步锁
        self._lock = asyncio.Lock()
        
        # 初始化
        asyncio.create_task(self._init())

    async def _init(self):
        """异步初始化"""
        await self._load_data()

    async def _load_data(self):
        """加载数据"""
        # 从JSON加载
        if self._json_backup.exists():
            try:
                data = json.loads(self._json_backup.read_text(encoding="utf-8"))
                if isinstance(data, list):
                    self._consented_users = set(data)
                elif isinstance(data, dict):
                    self._consented_users = set(data.get("users", []))
                    self._qq_groups = {k: set(v) for k, v in data.get("groups", {}).items()}
                logger.info(f"[UserAgreement] 加载 {len(self._consented_users)} 个已同意用户")
            except Exception as e:
                logger.error(f"[UserAgreement] 加载数据失败: {e}")

    async def _save_data(self):
        """保存数据"""
        async with self._lock:
            data = {
                "users": list(self._consented_users),
                "groups": {k: list(v) for k, v in self._qq_groups.items()}
            }
            self._json_backup.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )

    def _get_group_id(self, evt) -> Optional[str]:
        """获取QQ群号"""
        try:
            # 尝试多种方式获取群号
            if hasattr(evt, 'message_obj'):
                msg_obj = evt.message_obj
                if hasattr(msg_obj, 'group_id'):
                    return str(msg_obj.group_id)
                if hasattr(msg_obj, 'raw_message'):
                    raw = msg_obj.raw_message
                    if isinstance(raw, dict):
                        return str(raw.get('group_id', ''))
            if hasattr(evt, 'group_id'):
                return str(evt.group_id)
            return None
        except:
            return None

    def _get_sender_id(self, evt) -> str:
        """获取发送者ID"""
        try:
            if hasattr(evt, 'get_sender_id'):
                return str(evt.get_sender_id())
            if hasattr(evt, 'sender_id'):
                return str(evt.sender_id)
            if hasattr(evt, 'message_obj'):
                msg_obj = evt.message_obj
                if hasattr(msg_obj, 'sender'):
                    sender = msg_obj.sender
                    if hasattr(sender, 'user_id'):
                        return str(sender.user_id)
            return "unknown"
        except:
            return "unknown"

    def _is_admin(self, sender_id: str) -> bool:
        """检查是否为管理员"""
        admin_raw = self.config.get("admin_list", "")
        if not admin_raw:
            return False
        admins = [a.strip() for a in admin_raw.split("\n") if a.strip()]
        return sender_id in admins

    def _normalize_cmd(self, cmd: str) -> str:
        """标准化命令"""
        # 去除@提及
        import re
        cmd = re.sub(r'\[CQ:at,qq=\d+\]', '', cmd)
        cmd = cmd.replace("@", "").strip()
        return " ".join(cmd.lower().split())

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_all_message(self, event: AstrMessageEvent, *args):
        """主事件处理"""
        try:
            evt = args[0] if args else event
            
            sender_id = self._get_sender_id(evt)
            if sender_id == "unknown":
                return
            
            message_str = evt.message_str.strip() if hasattr(evt, 'message_str') else ""
            if not message_str:
                return
            
            group_id = self._get_group_id(evt)
            normalized_msg = self._normalize_cmd(message_str)
            
            # 处理同意命令
            if await self._handle_agree(normalized_msg, sender_id, group_id, evt):
                evt.stop_event()
                return
            
            # 处理撤销命令
            if await self._handle_revoke(normalized_msg, sender_id, evt):
                evt.stop_event()
                return
            
            # 处理统计命令
            if await self._handle_stats(normalized_msg, sender_id, group_id, evt):
                evt.stop_event()
                return
            
            # 检查是否已同意
            if sender_id in self._consented_users:
                return
            
            # 处理提醒
            await self._handle_reminder(sender_id, group_id, evt)
            evt.stop_event()
            
        except Exception as e:
            logger.error(f"[UserAgreement] 处理消息出错: {e}", exc_info=True)

    async def _handle_agree(self, normalized_msg: str, sender_id: str, group_id: Optional[str], evt) -> bool:
        """处理同意命令"""
        agree_cmd = self.config.get("agree_command", "同意")
        agree_cmds = ["/同意", "/agree", f"/{agree_cmd}", agree_cmd, "同意"]
        normalized_agree_cmds = {self._normalize_cmd(cmd) for cmd in agree_cmds}
        
        if normalized_msg in normalized_agree_cmds:
            if sender_id in self._consented_users:
                await evt.plain_result(self.config.get("already_agreed_text", "您已同意过用户协议"))
            else:
                self._consented_users.add(sender_id)
                if group_id:
                    if group_id not in self._qq_groups:
                        self._qq_groups[group_id] = set()
                    self._qq_groups[group_id].add(sender_id)
                await self._save_data()
                logger.info(f"[UserAgreement] 用户 {sender_id} 已同意")
                await evt.plain_result(self.config.get("agree_success_text", "✅ 已同意用户协议"))
            return True
        return False

    async def _handle_revoke(self, normalized_msg: str, sender_id: str, evt) -> bool:
        """处理撤销命令"""
        revoke_cmd = self.config.get("revoke_command", "撤销所有同意")
        revoke_cmds = ["/撤销所有同意", f"/{revoke_cmd}", revoke_cmd]
        normalized_revoke_cmds = {self._normalize_cmd(cmd) for cmd in revoke_cmds}
        
        if normalized_msg in normalized_revoke_cmds:
            if self._is_admin(sender_id):
                count = len(self._consented_users)
                self._consented_users.clear()
                self._qq_groups.clear()
                self._reminded_users.clear()
                await self._save_data()
                logger.info(f"[UserAgreement] 管理员 {sender_id} 撤销了 {count} 个同意")
                await evt.plain_result(self.config.get("revoke_success_text", f"✅ 已撤销 {count} 个用户的同意"))
            else:
                await evt.plain_result(self.config.get("revoke_no_admin_text", "❌ 无权限"))
            return True
        return False

    async def _handle_stats(self, normalized_msg: str, sender_id: str, group_id: Optional[str], evt) -> bool:
        """处理统计命令"""
        if normalized_msg in ["/协议统计", "协议统计"] and self._is_admin(sender_id):
            stats = [
                f"📊 统计信息",
                f"总同意用户: {len(self._consented_users)}",
                f"待提醒用户: {len(self._reminded_users)}",
                f"涉及群数: {len(self._qq_groups)}"
            ]
            if group_id:
                group_users = self._qq_groups.get(group_id, set())
                stats.append(f"本群同意用户: {len(group_users)}")
            await evt.plain_result("\n".join(stats))
            return True
        return False

    async def _handle_reminder(self, sender_id: str, group_id: Optional[str], evt):
        """处理提醒"""
        reminder_key = f"{group_id}:{sender_id}" if group_id else sender_id
        
        if reminder_key in self._reminded_users:
            await evt.plain_result(self.config.get("repeat_reminder_text", "请先同意用户协议"))
            return
        
        self._reminded_users.add(reminder_key)
        
        ua_title = self.config.get("user_agreement_title", "用户协议")
        pp_title = self.config.get("privacy_policy_title", "隐私政策")
        ua_text = self.config.get("user_agreement", "暂无内容")
        pp_text = self.config.get("privacy_policy", "暂无内容")
        
        reminder_text = self.config.get("reminder_text", "请先阅读并同意以下协议")
        
        # QQ平台直接发送文本
        full_text = f"{reminder_text}\n\n{'='*30}\n📄 {ua_title}\n{'='*30}\n{ua_text}\n\n{'='*30}\n🔒 {pp_title}\n{'='*30}\n{pp_text}"
        await evt.plain_result(full_text)