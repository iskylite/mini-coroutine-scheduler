import asyncio
import logging
import subprocess
from typing import Callable, Optional

from scheduler import CoroutineTask, TaskEvent, TaskResult

# 配置logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class ShellCommandTask(CoroutineTask):
    """
    Shell 命令任务类
    """

    def __init__(self, task_id: str, command: str, timeout: Optional[float] = None) -> None:
        """
        初始化 ShellCommandTask

        Args:
            task_id (str): 任务 ID
            command (str): 要执行的 Shell 命令
            timeout (Optional[float]): 命令执行超时时间，单位秒，默认为 None (不超时)
        """
        super().__init__(task_id)
        self.command: str = command
        self.timeout: Optional[float] = timeout
        self.process: Optional[subprocess.Process] = None

    async def initialize(self) -> None:
        """
        初始化任务
        """
        logging.info(f"Task {self.task_id} initializing with command: {self.command}")

    async def execute(self, callback: Callable[[TaskResult], None]) -> None:
        """
        执行任务

        Args:
            callback (Callable[[TaskResult], None]): 任务结果回调函数
        """
        logging.info(f"Task {self.task_id} executing command: {self.command}")
        try:
            self.process = await asyncio.create_subprocess_shell(
                self.command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )

            async def stream_output(stream, event_type):
                while True:
                    line = await stream.readline()
                    if not line:
                        break
                    line_str = line.decode("utf-8").strip()
                    logging.debug(f"Task {self.task_id} {event_type}: {line_str}")
                    result = TaskResult(task_id=self.task_id, event=event_type, data=line_str)
                    callback(result)

            stdout_task = asyncio.create_task(stream_output(self.process.stdout, TaskEvent.STDOUT))
            stderr_task = asyncio.create_task(stream_output(self.process.stderr, TaskEvent.STDERR))

            # 使用 asyncio.wait_for 设置超时
            await asyncio.wait_for(asyncio.wait([stdout_task, stderr_task]), timeout=self.timeout)

            return_code = await self.process.wait()

            if return_code == 0:
                result = TaskResult(
                    task_id=self.task_id,
                    event=TaskEvent.COMPLETED,
                    data=f"Command completed successfully with return code {return_code}",
                )
                callback(result)
                self.future.set_result(None)  # 标记任务完成
            else:
                error_message = f"Command failed with return code {return_code}"
                result = TaskResult(
                    task_id=self.task_id, event=TaskEvent.FAILED, error=error_message
                )
                callback(result)
                self.future.set_exception(Exception(error_message))  # 标记任务失败

        except asyncio.TimeoutError:
            logging.warning(f"Task {self.task_id} timed out")
            if self.process:
                self.process.terminate()
            result = TaskResult(
                task_id=self.task_id, event=TaskEvent.FAILED, error="Command timed out"
            )
            callback(result)
            self.future.set_exception(asyncio.TimeoutError("Command timed out"))  # 标记任务超时
        except Exception as e:
            logging.exception(f"Task {self.task_id} failed: {e}")
            result = TaskResult(task_id=self.task_id, event=TaskEvent.FAILED, error=str(e))
            callback(result)
            self.future.set_exception(e)  # 标记任务失败
        finally:
            if self.process and self.process.stdout:
                await self.process.stdout.wait_closed()
            if self.process and self.process.stderr:
                await self.process.stderr.wait_closed()

    async def cancel(self) -> None:
        """
        取消任务
        """
        logging.info(f"Task {self.task_id} cancelling")
        if self.process:
            self.process.terminate()
        self.future.cancel()
