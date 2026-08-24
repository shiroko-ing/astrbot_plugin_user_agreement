import json
import asyncio
import threading
from pathlib import Path
from typing import Optional, Set, Dict
from concurrent.futures import ThreadPoolExecutor

from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star
from astrbot.api import logger, AstrBotConfig
from astrbot.api.message_components import Node, Plain, Image
from astrbot.core.utils.astrbot_path import get_astrbot_data_path

try:
    import aiosqlite
    HAS_AIOSQLITE = True
except ImportError:
    HAS_AIOSQLITE = False
    logger.warning("[UserAgreement] 未安装 aiosqlite，将使用同步SQLite")


class UserAgreement(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self._lock = asyncio.Lock()
        self._thread_lock = threading.RLock()
        self._executor = ThreadPoolExecutor(max_workers=2)
        self._consented_users: Set[str] = set()
        self._reminded_users: Set[str] = set()
        self._db_path = None
        self._json_backup = None
        self._conn = None
        self._sync_conn = None
        
        # QQ平台特殊处理
        self._qq_groups: Dict[str, Set[str]] = {}  # 群号 -> 用户集合
        self._group_consent_mode = config.get("group_consent_mode", "individual")  # individual: 个人同意, group: 群统一同意
        
        # 初始化路径
        self._init_paths()
        
        # 异步初始化
        asyncio.create_task(self._async_init())

    def _init_paths(self):
        """初始化路径"""
        data_path = Path(get_astrbot_data_path()) / "plugin_data" / self.name
        data_path.mkdir(parents=True, exist_ok=True)
        self._db_path = data_path / "consents.db"
        self._json_backup = data_path / "consented_users_backup.json"

    async def _async_init(self):
        """异步初始化"""
        await self._init_database()
        await self._load_data()
        self._validate_config()

    async def _init_database(self):
        """初始化数据库"""
        if HAS_AIOSQLITE:
            try:
                self._conn = await aiosqlite.connect(str(self._db_path))
                await self._conn.execute("""
                    CREATE TABLE IF NOT EXISTS consents (
                        user_id TEXT PRIMARY KEY,
                        consented_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        platform TEXT DEFAULT 'qq',
                        group_id TEXT
                    )
                """)
                await self._conn.execute("""
                    CREATE TABLE IF NOT EXISTS reminded_users (
                        user_id TEXT PRIMARY KEY,
                        reminded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        group_id TEXT
                    )
                """)
                await self._conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_consents_group 
                    ON consents(group_id)
                """)
                await self._conn.commit()
                logger.info("[UserAgreement] 异步数据库初始化成功")
                return
            except Exception as e:
                logger.error(f"[UserAgreement] 异步数据库初始化失败: {e}")
                self._conn = None
        
        # 回退到同步数据库
        await self._init_sync_database()

    async def _init_sync_database(self):
        """初始化同步数据库"""
        def _init():
            import sqlite3
            self._sync_conn = sqlite3.connect(
                str(self._db_path), 
                check_same_thread=False,
                timeout=30
            )
            self._sync_conn.execute("""
                CREATE TABLE IF NOT EXISTS consents (
                    user_id TEXT PRIMARY KEY,
                    consented_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    platform TEXT DEFAULT 'qq',
                    group_id TEXT
                )
            """)
            self._sync_conn.execute("""
                CREATE TABLE IF NOT EXISTS reminded_users (
                    user_id TEXT PRIMARY KEY,
                    reminded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    group_id TEXT
                )
            """)
            self._sync_conn.commit()
        
        try:
            await asyncio.get_event_loop().run_in_executor(self._executor, _init)
            logger.info("[UserAgreement] 同步数据库初始化成功")
        except Exception as e:
            logger.error(f"[UserAgreement] 同步数据库初始化失败: {e}")

    async def _load_data(self):
        """加载数据"""
        # 从数据库加载
        if self._conn:
            try:
                async with self._conn.execute(
                    "SELECT user_id, group_id FROM consents"
                ) as cursor:
                    rows = await cursor.fetchall()
                    for row in rows:
                        user_id = row[0]
                        group_id = row[1]
                        self._consented_users.add(user_id)
                        if group_id:
                            if group_id not in self._qq_groups:
                                self._qq_groups[group_id] = set()
                            self._qq_groups[group_id].add(user_id)
                logger.info(f"[UserAgreement] 从数据库加载 {len(self._consented_users)} 个用户")
                return
            except Exception as e:
                logger.error(f"[UserAgreement] 数据库加载失败: {e}")
        
        # 从JSON备份加载
        if self._json_backup and self._json_backup.exists():
            try:
                def _load_json():
                    data = json.loads(self._json_backup.read_text(encoding="utf-8"))
                    if isinstance(data, list):
                        return set(data), {}
                    elif isinstance(data, dict):
                        return set(data.get("users", [])), data.get("groups", {})
                    return set(), {}
                
                users, groups = await asyncio.get_event_loop().run_in_executor(
                    self._executor, _load_json
                )
                self._consented_users = users
                self._qq_groups = {k: set(v) for k, v in groups.items()}
                logger.info(f"[UserAgreement] 从JSON备份加载 {len(self._consented_users)} 个用户")
            except Exception as e:
                logger.error(f"[UserAgreement] JSON备份加载失败: {e}")

    def _get_group_id(self, evt) -> Optional[str]:
        """获取QQ群号"""
        try:
            # 尝试不同的方式获取群号
            if hasattr(evt.message_obj, 'group_id'):
                return str(evt.message_obj.group_id)
            elif hasattr(evt.message_obj, 'group'):
                group = evt.message_obj.group
                if hasattr(group, 'id'):
                    return str(group.id)
                elif isinstance(group, dict):
                    return str(group.get('id', ''))
            elif hasattr(evt, 'group_id'):
                return str(evt.group_id)
            
            # 从消息对象中获取
            if hasattr(evt.message_obj, 'raw_message'):
                raw = evt.message_obj.raw_message
                if isinstance(raw, dict):
                    return str(raw.get('group_id', ''))
            
            # 从session中获取
            if hasattr(evt, 'session'):
                session = evt.session
                if hasattr(session, 'group_id'):
                    return str(session.group_id)
                elif isinstance(session, dict):
                    return str(session.get('group_id', ''))
            
            return None
        except Exception as e:
            logger.debug(f"[UserAgreement] 获取群号失败: {e}")
            return None

    def _is_group_message(self, evt) -> bool:
        """判断是否为群消息"""
        group_id = self._get_group_id(evt)
        return group_id is not None and group_id != ""

    def _get_platform(self, evt) -> str:
        """获取平台类型"""
        try:
            # 尝试从事件中获取平台信息
            if hasattr(evt, 'platform'):
                return str(evt.platform)
            if hasattr(evt.message_obj, 'platform'):
                return str(evt.message_obj.platform)
            # 默认为QQ
            return "qq"
        except:
            return "qq"

    async def _save_consent(self, user_id: str, group_id: Optional[str] = None):
        """保存用户同意"""
        async with self._lock:
            self._consented_users.add(user_id)
            
            # 记录群信息
            if group_id:
                if group_id not in self._qq_groups:
                    self._qq_groups[group_id] = set()
                self._qq_groups[group_id].add(user_id)
            
            # 保存到数据库
            if self._conn:
                try:
                    await self._conn.execute(
                        """INSERT OR REPLACE INTO consents (user_id, group_id) 
                           VALUES (?, ?)""",
                        (user_id, group_id)
                    )
                    await self._conn.commit()
                except Exception as e:
                    logger.error(f"[UserAgreement] 数据库保存失败: {e}")
            
            elif self._sync_conn:
                def _save():
                    self._sync_conn.execute(
                        """INSERT OR REPLACE INTO consents (user_id, group_id) 
                           VALUES (?, ?)""",
                        (user_id, group_id)
                    )
                    self._sync_conn.commit()
                
                try:
                    await asyncio.get_event_loop().run_in_executor(self._executor, _save)
                except Exception as e:
                    logger.error(f"[UserAgreement] 同步数据库保存失败: {e}")
            
            # 每10次保存一次JSON备份
            if len(self._consented_users) % 10 == 0:
                await self._save_json_backup()

    async def _save_json_backup(self):
        """保存JSON备份"""
        def _save():
            if self._json_backup:
                data = {
                    "users": list(self._consented_users),
                    "groups": {k: list(v) for k, v in self._qq_groups.items()}
                }
                self._json_backup.write_text(
                    json.dumps(data, ensure_ascii=False, indent=2),
                    encoding="utf-8"
                )
        
        try:
            await asyncio.get_event_loop().run_in_executor(self._executor, _save)
        except Exception as e:
            logger.error(f"[UserAgreement] JSON备份保存失败: {e}")

    async def _clear_all_data(self):
        """清除所有数据"""
        async with self._lock:
            count = len(self._consented_users)
            self._consented_users.clear()
            self._qq_groups.clear()
            self._reminded_users.clear()
            
            # 清除数据库
            if self._conn:
                try:
                    await self._conn.execute("DELETE FROM consents")
                    await self._conn.execute("DELETE FROM reminded_users")
                    await self._conn.commit()
                except Exception as e:
                    logger.error(f"[UserAgreement] 数据库清除失败: {e}")
            
            elif self._sync_conn:
                def _clear():
                    self._sync_conn.execute("DELETE FROM consents")
                    self._sync_conn.execute("DELETE FROM reminded_users")
                    self._sync_conn.commit()
                
                try:
                    await asyncio.get_event_loop().run_in_executor(self._executor, _clear)
                except Exception as e:
                    logger.error(f"[UserAgreement] 同步数据库清除失败: {e}")
            
            # 清除JSON备份
            await self._save_json_backup()
            return count

    def _is_admin(self, sender_id: str) -> bool:
        """检查是否为管理员"""
        admin_raw = self.config.get("admin_list", "")
        if not admin_raw:
            return False
        admins = [a.strip() for a in admin_raw.split("\n") if a.strip()]
        return sender_id in admins

    def _get_cmd(self, name: str, default: str = "") -> str:
        """获取配置"""
        return self.config.get(name, default)

    def _normalize_cmd(self, cmd: str) -> str:
        """标准化命令"""
        # QQ消息可能有@提及，需要处理
        # 去除@提及
        cmd = cmd.replace("@", "").strip()
        return " ".join(cmd.lower().split())

    def _should_check_consent(self, evt) -> bool:
        """判断是否需要检查同意状态"""
        # 可以配置哪些群需要检查
        check_groups = self.config.get("check_groups", "")
        if check_groups:
            group_id = self._get_group_id(evt)
            if group_id:
                check_list = [g.strip() for g in check_groups.split("\n") if g.strip()]
                return group_id in check_list
            return False
        return True

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_all_message(self, event: AstrMessageEvent, *args):
        """主事件处理"""
        try:
            evt = args[0] if args else event
            
            # 获取发送者ID
            try:
                sender_id = str(evt.message_obj.sender.user_id)
            except (AttributeError, Exception):
                return
            
            message_str = evt.message_str.strip()
            if not message_str:
                return
            
            # 获取群信息
            group_id = self._get_group_id(evt)
            platform = self._get_platform(evt)
            
            # 检查是否需要处理
            if not self._should_check_consent(evt):
                return
            
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
            if self._has_consented(sender_id, group_id):
                return
            
            # 处理提醒
            await self._handle_reminder(sender_id, group_id, evt)
            evt.stop_event()
            
        except Exception as e:
            logger.error(f"[UserAgreement] 处理消息出错: {e}", exc_info=True)

    def _has_consented(self, user_id: str, group_id: Optional[str]) -> bool:
        """检查是否已同意"""
        # 个人模式
        if user_id in self._consented_users:
            return True
        
        # 群模式（如果启用）
        if self._group_consent_mode == "group" and group_id:
            # 检查群是否已同意（群主或管理员代表群同意）
            return group_id in self._qq_groups and len(self._qq_groups[group_id]) > 0
        
        return False

    async def _handle_agree(self, normalized_msg: str, sender_id: str, group_id: Optional[str], evt) -> bool:
        """处理同意命令"""
        agree_cmd = self._get_cmd("agree_command", "同意")
        agree_cmds = ["/同意", "/agree", f"/{agree_cmd}", agree_cmd, "同意"]
        normalized_agree_cmds = {self._normalize_cmd(cmd) for cmd in agree_cmds}
        
        if normalized_msg in normalized_agree_cmds:
            if self._has_consented(sender_id, group_id):
                await evt.plain_result(
                    self._get_cmd("already_agreed_text", "您已同意过用户协议")
                )
            else:
                await self._save_consent(sender_id, group_id)
                logger.info(f"[UserAgreement] 用户 {sender_id} 在群 {group_id} 已同意")
                await evt.plain_result(
                    self._get_cmd("agree_success_text", "✅ 已同意用户协议")
                )
            return True
        return False

    async def _handle_revoke(self, normalized_msg: str, sender_id: str, evt) -> bool:
        """处理撤销命令"""
        revoke_cmd = self._get_cmd("revoke_command", "撤销所有同意")
        revoke_cmds = ["/撤销所有同意", f"/{revoke_cmd}", revoke_cmd]
        normalized_revoke_cmds = {self._normalize_cmd(cmd) for cmd in revoke_cmds}
        
        if normalized_msg in normalized_revoke_cmds:
            if self._is_admin(sender_id):
                count = await self._clear_all_data()
                logger.info(f"[UserAgreement] 管理员 {sender_id} 撤销了 {count} 个同意")
                await evt.plain_result(
                    self._get_cmd("revoke_success_text", f"✅ 已撤销 {count} 个用户的同意")
                )
            else:
                await evt.plain_result(
                    self._get_cmd("revoke_no_admin_text", "❌ 无权限")
                )
            return True
        return False

    async def _handle_stats(self, normalized_msg: str, sender_id: str, group_id: Optional[str], evt) -> bool:
        """处理统计命令"""
        if normalized_msg == "/协议统计" or normalized_msg == "协议统计":
            if self._is_admin(sender_id):
                stats = [
                    f"📊 统计信息",
                    f"总同意用户: {len(self._consented_users)}",
                    f"待提醒用户: {len(self._reminded_users)}",
                ]
                
                if group_id:
                    group_users = self._qq_groups.get(group_id, set())
                    stats.append(f"本群同意用户: {len(group_users)}")
                
                stats.append(f"涉及群数: {len(self._qq_groups)}")
                
                await evt.plain_result("\n".join(stats))
                return True
        return False

    async def _handle_reminder(self, sender_id: str, group_id: Optional[str], evt):
        """处理提醒"""
        reminder_key = f"{group_id}:{sender_id}" if group_id else sender_id
        
        if reminder_key in self._reminded_users:
            await evt.plain_result(
                self._get_cmd("repeat_reminder_text", "请先同意用户协议")
            )
            return
        
        self._reminded_users.add(reminder_key)
        
        ua_title = self._get_cmd("user_agreement_title", "用户协议")
        pp_title = self._get_cmd("privacy_policy_title", "隐私政策")
        
        ua_text = self._get_cmd("user_agreement", "暂无内容")
        pp_text = self._get_cmd("privacy_policy", "暂无内容")
        
        # QQ平台使用文字消息而不是Node
        reminder_text = self._get_cmd(
            "reminder_text",
            "📋 请先阅读并同意以下协议：\n\n发送 /同意 表示同意"
        )
        
        # 对于QQ，直接发送文本而不是Node
        if self._get_platform(evt) == "qq":
            full_text = f"{reminder_text}\n\n{'='*30}\n📄 {ua_title}\n{'='*30}\n{ua_text}\n\n{'='*30}\n🔒 {pp_title}\n{'='*30}\n{pp_text}"
            await evt.plain_result(full_text)
        else:
            # 其他平台使用Node
            node_ua = Node(uin=sender_id, name=ua_title, content=[Plain(ua_text)])
            node_pp = Node(uin=sender_id, name=pp_title, content=[Plain(pp_text)])
            await evt.plain_result(reminder_text)
            await evt.chain_result([node_ua, node_pp])

    def _validate_config(self):
        """验证配置"""
        required_configs = [
            "agree_command", "revoke_command", "already_agreed_text",
            "agree_success_text", "revoke_success_text", "revoke_no_admin_text",
            "repeat_reminder_text", "reminder_text", "user_agreement_title",
            "privacy_policy_title", "user_agreement", "privacy_policy"
        ]
        
        missing = [cfg for cfg in required_configs if not self.config.get(cfg)]
        if missing:
            logger.warning(f"[UserAgreement] 缺少配置项: {', '.join(missing)}")

    async def __aenter__(self):
        """异步上下文管理器入口"""
        await self._async_init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器退出"""
        if self._conn:
            await self._conn.close()
        if self._sync_conn:
            self._sync_conn.close()
        self._executor.shutdown(wait=False)