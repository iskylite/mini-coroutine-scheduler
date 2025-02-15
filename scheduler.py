import asyncio
import concurrent.futures
import logging
import os
import subprocess
import uuid
from abc import ABC, abstractmethod
from collections import deque
from enum import Enum, auto
from typing import Any, AsyncGenerator, Callable, Coroutine, Dict, List, Optional, Protocol, Set, Tuple, Type, Union

# 配置logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TaskEvent(str, Enum):
    """
    任务事件类型
    """
    STARTED = "started"
    INITIALIZED = "initialized"
    EXECUTED = "executed"
    STDOUT = "stdout"  # 标准输出
    STDERR = "stderr"  # 标准错误
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"

class TaskResult:
    """
    任务结果类
    """
    def __init__(self, task_id: str, event: TaskEvent, data: Any = None, error: Optional[str] = None):
        """
        初始化任务结果
        
        Args:
            task_id (str): 任务 ID
            event (TaskEvent): 任务事件类型
            data (Any): 任务数据
            error (Optional[str]): 任务错误信息
        """
        self.task_id: str = task_id
        self.event: TaskEvent = event
        self.data: Any = data
        self.error: Optional[str] = error

    def __repr__(self) -> str:
        return f"TaskResult(task_id='{self.task_id}', event='{self.event}', data={self.data}, error='{self.error}')"

class CoroutineTask(ABC):
    """
    协程任务抽象基类
    """
    def __init__(self, task_id: str) -> None:
        """
        初始化协程任务
        
        Args:
            task_id (str): 任务 ID
        """
        self.task_id: str = task_id
        self._future: asyncio.Future[None] = asyncio.Future()

    @property
    def future(self) -> asyncio.Future[None]:
        """
        获取任务的 future 对象
        
        Returns:
            asyncio.Future[None]: 任务的 future 对象，用于跟踪任务的完成状态。
                                  可以通过 await task.future 等待任务完成，
                                  也可以通过 task.future.cancel() 取消任务。
        """
        return self._future

    @abstractmethod
    async def initialize(self) -> None:
        """
        初始化任务
        """
        ...

    @abstractmethod
    async def execute(self, callback: Callable[[TaskResult], None]) -> None:
        """
        执行任务，并通过回调函数返回任务结果
        
        Args:
            callback (Callable[[TaskResult], None]): 任务结果回调函数
        """
        ...

    @abstractmethod
    async def cancel(self) -> None:
        """
        取消/结束任务
        """
        ...

class TaskGroupResultHandler(Protocol):
    """
    任务组结果处理接口
    """
    async def process_results(self, group_id: str, results: List[TaskResult]) -> None:
        """
        处理任务组结果
        
        Args:
            group_id (str): 任务组 ID
            results (List[TaskResult]): 任务结果列表
        """
        ...

class TaskGroupStatus(Enum):
    """
    任务组状态
    """
    ACTIVE = auto()  # 任务组正在运行
    COMPLETED = auto()  # 任务组已完成
    DEACTIVE = auto()  # 任务组已失效

class CoroutineScheduler:
    """
    协程调度器
    """
    def __init__(self, 
                 max_workers: Optional[int] = None,
                 max_queue_size: int = 1000, 
                 result_max_size: int = 100, 
                 result_handler: Optional[TaskGroupResultHandler] = None,
                 cache_last_result: bool = False) -> None:
        """
        初始化协程调度器
        
        Args:
            max_workers (Optional[int]): 最大并发数，默认为 CPU 核心数
            max_queue_size (int): 任务队列最大长度，默认为 1000
            result_max_size (int): 结果队列最大长度，默认为 100
            result_handler (Optional[TaskGroupResultHandler]): 任务组结果处理器，默认为 None
            cache_last_result (bool): 是否缓存最新结果，默认为 False
        """
        self.max_workers: int = max_workers if max_workers is not None else os.cpu_count()  # 最大并发数
        self.task_queue: asyncio.Queue[Tuple[Type[CoroutineTask], str, Dict[str, Any]]] = asyncio.Queue(maxsize=max_queue_size)  # 任务队列，存储任务类、group_id和初始化参数
        self.running_tasks: Dict[str, CoroutineTask] = {}  # 正在运行的任务，使用task_id作为key
        self.task_groups: Dict[str, str] = {}  # 任务组，存储group_id和task_id的映射
        self.result_queues: Dict[str, deque[TaskResult]] = {}  # 结果队列，用于协程管道, 使用deque
        self.result_max_size: int = result_max_size  # 结果队列最大长度
        self.task_completion_events: Dict[str, asyncio.Event] = {}  # 任务组完成事件
        self.executor: concurrent.futures.ThreadPoolExecutor = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) # 线程池执行CPU密集型任务
        self.scheduler_running: bool = False  # 调度器运行状态
        self.total_tasks: int = 0  # 任务总数
        self.result_handler: Optional[TaskGroupResultHandler] = result_handler  # 任务组结果处理器
        self.group_cancelled: Set[str] = set()  # 记录已取消的任务组
        self.cache_last_result: bool = cache_last_result  # 是否缓存最新结果
        self.last_results: Dict[str, TaskResult] = {}  # 存储每个任务ID的最新结果
        self._shutdown_event: asyncio.Event = asyncio.Event()  # 用于优雅关闭调度器
        self.task_group_configs: Dict[str, Dict[str, Any]] = {}  # 存储任务组配置信息
        self.task_group_status: Dict[str, TaskGroupStatus] = {}  # 存储任务组状态

    async def start(self) -> None:
        """
        启动调度器
        """
        if self.scheduler_running:
            logging.warning("Scheduler is already running.")
            return
        self.scheduler_running = True
        self._shutdown_event.clear()  # 确保关闭事件被清除
        self.workers: List[asyncio.Task[Any]] = [asyncio.create_task(self._worker(), name=f"worker-{i}") for i in range(self.max_workers)]  # 命名工作协程
        logging.info("Scheduler started with max_workers=%d", self.max_workers)

    async def stop(self, cancel_running: bool = True, timeout: float = 10.0) -> None:
        """
        停止调度器
        
        Args:
            cancel_running (bool): 是否取消正在运行的任务，默认为 True
            timeout (float): 等待任务完成的超时时间，默认为 10.0 秒
        """
        if not self.scheduler_running:
            logging.warning("Scheduler is not running.")
            return

        self.scheduler_running = False
        logging.info("Scheduler stopping...")

        # 设置关闭事件，通知worker停止
        self._shutdown_event.set()

        if cancel_running:
            logging.info("Cancelling running tasks...")
            await self._cancel_all_running_tasks()

        # 等待所有worker完成
        try:
            await asyncio.wait_for(asyncio.gather(*self.workers, return_exceptions=True), timeout=timeout)
        except asyncio.TimeoutError:
            logging.error("Stopping scheduler timed out, some workers may not have finished.")
        finally:
            self.executor.shutdown(wait=True)  # 关闭线程池
            logging.info("Scheduler stopped")

    async def _cancel_all_running_tasks(self) -> None:
        """
        取消所有正在运行的任务
        """
        for task_id in list(self.running_tasks.keys()):
            await self.cancel_task(task_id)

    async def register_task(self, task_class: Type[CoroutineTask], group_id: str, task_id: str, max_tasks: int, **kwargs: Any) -> str:
        """
        注册任务到调度器
        
        Args:
            task_class (Type[CoroutineTask]): 任务类
            group_id (str): 任务组 ID
            task_id (str): 任务 ID，作为协程任务的 key
            max_tasks (int): 任务组最大任务数
            **kwargs (Any): 任务初始化参数
        
        Returns:
            str: 任务 ID
        
        Raises:
            ValueError: 协程任务 ID 重复
            MemoryError: 任务组已达到最大任务数
        """
        if task_id in self.task_groups:
            raise ValueError(f"Coroutine task ID {task_id} already exists.")

        if group_id in self.task_group_status and self.task_group_status[group_id] == TaskGroupStatus.DEACTIVE:
            self._reuse_deactive_group(group_id, max_tasks)

        if group_id not in self.task_group_configs:
            self._create_task_group(group_id, max_tasks)
        else:
            if len(self.task_groups) >= self.task_group_configs[group_id]["max_tasks"]:
                raise MemoryError(f"Task group {group_id} has reached its maximum task limit.")

        self.task_groups[group_id] = task_id  # 存储 group_id 和 task_id 的映射
        try:
            await self._put_task_in_queue(task_class, group_id, task_id, **kwargs)
            return task_id
        except asyncio.QueueFull:
            logging.error(f"Task queue is full, cannot add task {task_id}")
            raise

    def _create_task_group(self, group_id: str, max_tasks: int) -> None:
        """
        创建任务组
        
        Args:
            group_id (str): 任务组 ID
            max_tasks (int): 任务组最大任务数
        """
        self.result_queues[group_id] = deque(maxlen=self.result_max_size)  # 创建结果队列，并设置最大长度
        self.task_completion_events[group_id] = asyncio.Event()  # 创建任务组完成事件
        self.task_group_configs[group_id] = {"max_tasks": max_tasks}
        self.task_group_status[group_id] = TaskGroupStatus.ACTIVE
        logging.info(f"Task group {group_id} created with max_tasks={max_tasks}")

    def _reuse_deactive_group(self, group_id: str, max_tasks: int) -> None:
        """
        复用 DEACTIVE 的任务组
        
        Args:
            group_id (str): 任务组 ID
            max_tasks (int): 任务组最大任务数
        
        Raises:
            ValueError: 任务组不是 DEACTIVE 状态
        """
        if self.task_group_status[group_id] != TaskGroupStatus.DEACTIVE:
            raise ValueError(f"Task group {group_id} is not DEACTIVE and cannot be reused.")

        # 清理任务组数据
        self._cleanup_group_data(group_id)
        self.task_group_configs[group_id] = {"max_tasks": max_tasks}
        self.task_group_status[group_id] = TaskGroupStatus.ACTIVE
        logging.info(f"Task group {group_id} reused with max_tasks={max_tasks}")

    def _cleanup_group_data(self, group_id: str) -> None:
        """
        清理任务组数据
        
        Args:
            group_id (str): 任务组 ID
        """
        # 取消所有任务
        asyncio.run(self.cancel_task(self.task_groups[group_id]))  # 同步调用取消任务

        self.result_queues[group_id].clear()
        self.task_completion_events[group_id] = asyncio.Event()
        logging.info(f"Task group {group_id} data cleaned up.")

    async def _put_task_in_queue(self, task_class: Type[CoroutineTask], group_id: str, task_id: str, **kwargs: Any) -> None:
        """
        将任务放入任务队列
        
        Args:
            task_class (Type[CoroutineTask]): 任务类
            group_id (str): 任务组 ID
            task_id (str): 任务 ID
            **kwargs (Any): 任务初始化参数
        """
        await self.task_queue.put((task_class, group_id, {"task_id": task_id, **kwargs}))
        self.total_tasks += 1
        logging.debug(f"Task {task_id} registered to group {group_id}")

    async def _worker(self) -> None:
        """
        工作协程，从任务队列中获取任务并执行
        """
        while self.scheduler_running and not self._shutdown_event.is_set():
            try:
                task_data: Optional[Tuple[Type[CoroutineTask], str, Dict[str, Any]]] = await self._get_task_from_queue()
                if task_data is None:
                    continue  # 如果队列为空，则继续循环

                task_class, group_id, kwargs = task_data
                task_id: str = kwargs["task_id"]

                if self.task_group_status.get(group_id) != TaskGroupStatus.ACTIVE:
                    logging.warning(f"Task {task_id} in group {group_id} is not ACTIVE, skipping execution.")
                    continue

                logging.debug(f"Task {task_id} pulled from queue")
                task: CoroutineTask = task_class(task_id=task_id)  # 初始化任务
                self.running_tasks[task_id] = task

                await self._execute_task(task, group_id)

            except asyncio.CancelledError:
                logging.info("Worker cancelled")
                break
            except Exception as e:
                logging.exception(f"Worker exception: {e}")

    async def _get_task_from_queue(self) -> Optional[Tuple[Type[CoroutineTask], str, Dict[str, Any]]]:
        """
        从任务队列中获取任务，如果队列为空则返回 None
        
        Returns:
            Optional[Tuple[Type[CoroutineTask], str, Dict[str, Any]]]: 任务数据，包含任务类、任务组 ID 和任务初始化参数
        """
        try:
            return await asyncio.wait_for(self.task_queue.get(), timeout=1.0)  # 设置超时时间
        except asyncio.TimeoutError:
            return None

    async def _execute_task(self, task: CoroutineTask, group_id: str) -> None:
        """
        执行任务
        
        Args:
            task (CoroutineTask): 任务对象
            group_id (str): 任务组 ID
        """
        task_id: str = task.task_id
        try:
            # 发送任务开始事件
            await self._send_result(task_id, group_id, TaskEvent.STARTED)

            logging.info(f"Task {task_id} started")
            
            # 发送任务初始化事件
            await self._send_result(task_id, group_id, TaskEvent.INITIALIZED)
            await task.initialize()

            # 执行任务
            await self._send_result(task_id, group_id, TaskEvent.EXECUTED)
            await task.execute(self._create_result_callback(task, group_id))
            await task.future  # 等待任务完成
            
            # 发送任务完成事件
            await self._send_result(task_id, group_id, TaskEvent.COMPLETED)

        except asyncio.CancelledError:
            logging.info(f"Task {task_id} cancelled")
            await self._send_result(task_id, group_id, TaskEvent.CANCELLED)
        except Exception as e:
            logging.exception(f"Task {task_id} failed: {e}")
            await self._send_result(task_id, group_id, TaskEvent.FAILED, error=str(e))
        finally:
            await self._cleanup_task(task_id, group_id)

    async def _cleanup_task(self, task_id: str, group_id: str) -> None:
        """
        清理任务
        
        Args:
            task_id (str): 任务 ID
            group_id (str): 任务组 ID
        """
        if task_id in self.running_tasks:
            del self.running_tasks[task_id]
        self.task_queue.task_done()
        await self._check_group_completion(group_id)  # 检查任务组是否完成

    async def _check_group_completion(self, group_id: str) -> None:
        """
        检查任务组是否完成，如果完成则处理结果并设置事件
        
        Args:
            group_id (str): 任务组 ID
        """
        if group_id in self.group_cancelled:
            return  # 任务组已经被取消，不再处理

        all_tasks_finished: bool = all(task_id not in self.running_tasks for task_id in self.task_groups.values())
        if all_tasks_finished and self.task_queue.empty():
            if not self.task_completion_events[group_id].is_set():
                self.task_completion_events[group_id].set()
                logging.info(f"Task group {group_id} completed")
                self.task_group_status[group_id] = TaskGroupStatus.COMPLETED
                await self._process_group_results(group_id)  # 处理任务组结果
                await self._deactivate_group(group_id)

    async def _deactivate_group(self, group_id: str) -> None:
        """
        标记任务组为 DEACTIVE
        
        Args:
            group_id (str): 任务组 ID
        """
        if self.task_group_status[group_id] != TaskGroupStatus.COMPLETED:
            logging.warning(f"Task group {group_id} is not COMPLETED, cannot be deactivated.")
            return

        if len(self.task_groups) == self.task_group_configs[group_id]["max_tasks"] and not self.result_queues[group_id]:
            self.task_group_status[group_id] = TaskGroupStatus.DEACTIVE
            logging.info(f"Task group {group_id} deactivated.")

    def _create_result_callback(self, task: CoroutineTask, group_id: str) -> Callable[[TaskResult], None]:
        """
        创建任务结果回调函数，避免每次都创建新的协程
        
        Args:
            task (CoroutineTask): 任务对象
            group_id (str): 任务组 ID
        
        Returns:
            Callable[[TaskResult], None]: 任务结果回调函数
        """
        def result_callback(result: TaskResult) -> None:
            """
            任务结果回调函数，将结果放入结果队列
            
            Args:
                result (TaskResult): 任务结果
            """
            try:
                asyncio.run(self._send_result(task.task_id, group_id, result.event, data=result.data, error=result.error))
            except Exception as e:
                logging.error(f"Error sending result for task {task.task_id}: {e}")
        return result_callback

    async def _send_result(self, task_id: str, group_id: str, event: TaskEvent, data: Any = None, error: Optional[str] = None) -> None:
        """
        发送任务结果到结果队列
        
        Args:
            task_id (str): 任务 ID
            group_id (str): 任务组 ID
            event (TaskEvent): 任务事件类型
            data (Any): 任务数据
            error (Optional[str]): 任务错误信息
        """
        if self.task_group_status.get(group_id) != TaskGroupStatus.ACTIVE:
            logging.warning(f"Task group {group_id} is not ACTIVE, cannot send result for task {task_id}.")
            return

        result: TaskResult = TaskResult(task_id, event, data, error)
        if group_id in self.result_queues:
            self.result_queues[group_id].append(result)
        else:
            logging.error(f"Result queue for group {group_id} not found.")

        if self.cache_last_result:
            self.last_results[task_id] = result  # 缓存最新结果

    async def cancel_task(self, task_id: str, force: bool = False) -> None:
        """
        取消任务
        
        Args:
            task_id (str): 任务 ID
            force (bool): 是否强制取消任务，默认为 False
        """
        logging.info(f"Attempting to cancel task {task_id}, force={force}")
        
        group_id: Optional[str] = next((group_id for group_id, task_id_in_group in self.task_groups.items() if task_id_in_group == task_id), None)
        if not group_id:
            logging.warning(f"Task {task_id} not found")
            return

        if task_id in self.running_tasks:
            await self._cancel_running_task(task_id, group_id, force)
        else:
            await self._cancel_queued_task(task_id, group_id)

    async def _cancel_running_task(self, task_id: str, group_id: str, force: bool) -> None:
        """
        取消正在运行的任务
        
        Args:
            task_id (str): 任务 ID
            group_id (str): 任务组 ID
            force (bool): 是否强制取消任务，默认为 False
        """
        task: CoroutineTask = self.running_tasks[task_id]
        try:
            await task.cancel()
            if force:
                task.future.cancel()  # 强制取消 future
            else:
                asyncio.create_task(self._wait_and_cancel(task.future))  # 异步等待并取消
        except Exception as e:
            logging.error(f"Error cancelling task {task_id}: {e}")
        finally:
            del self.running_tasks[task_id]
            logging.info(f"Task {task_id} cancelled")
            await self._send_result(task_id, group_id, TaskEvent.CANCELLED, error="Task cancelled by user")

    async def _cancel_queued_task(self, task_id: str, group_id: str) -> None:
        """
        取消队列中的任务
        
        Args:
            task_id (str): 任务 ID
            group_id (str): 任务组 ID
        """
        temp_queue: deque[Tuple[Type[CoroutineTask], str, Dict[str, Any]]] = deque()
        task_removed: bool = False
        while not self.task_queue.empty():
            try:
                task_item: Tuple[Type[CoroutineTask], str, Dict[str, Any]]] = self.task_queue.get_nowait()
                task_class: Type[CoroutineTask]
                item_group_id: str
                kwargs: Dict[str, Any]
                task_class, item_group_id, kwargs = task_item
                if kwargs.get("task_id") == task_id:
                    logging.info(f"Task {task_id} cancelled from queue")
                    await self._send_result(task_id, group_id, TaskEvent.CANCELLED, error="Task cancelled by user")
                    task_removed = True
                else:
                    temp_queue.append(task_item)
            except asyncio.QueueEmpty:
                break
        
        # 将未取消的任务放回队列
        while temp_queue:
            task_item = temp_queue.popleft()
            await self.task_queue.put(task_item)
        
        if task_removed:
            return

    async def _wait_and_cancel(self, future: asyncio.Future[None], timeout: float = 5.0) -> None:
        """
        等待一段时间后取消 future
        
        Args:
            future (asyncio.Future[None]): 任务的 future 对象
            timeout (float): 等待超时时间，默认为 5.0 秒
        """
        try:
            await asyncio.sleep(timeout)
            future.cancel()
            logging.info("Future cancelled after timeout.")
        except asyncio.CancelledError:
            logging.info("Future was already cancelled.")

    async def cancel_group(self, group_id: str) -> None:
        """
        取消任务组
        
        Args:
            group_id (str): 任务组 ID
        """
        if group_id not in self.task_groups:
            logging.warning(f"Task group {group_id} not found")
            return

        if group_id in self.group_cancelled:
            return  # 任务组已经被取消

        self.group_cancelled.add(group_id)  # 标记任务组为已取消

        task_id: str = self.task_groups[group_id]
        await self.cancel_task(task_id)

        # 清理结果队列
        if group_id in self.result_queues:
            self.result_queues[group_id].clear()
            logging.info(f"Result queue for group {group_id} cleared")

        # 移除任务组
        del self.task_groups[group_id]
        logging.info(f"Task group {group_id} cancelled and removed")

    async def deactivate_group(self, group_id: str) -> None:
        """
        主动标记任务组失活
        
        Args:
            group_id (str): 任务组 ID
        """
        if group_id not in self.task_group_status:
            logging.warning(f"Task group {group_id} not found")
            return

        if self.task_group_status[group_id] == TaskGroupStatus.DEACTIVE:
            logging.warning(f"Task group {group_id} is already DEACTIVE")
            return

        self.task_group_status[group_id] = TaskGroupStatus.COMPLETED  # 先标记为 COMPLETED
        await self._deactivate_group(group_id)  # 再尝试标记为 DEACTIVE

    def get_result(self, task_id: str, use_last_result: bool = False) -> Optional[TaskResult]:
        """
        获取指定任务的结果，直到任务完成 (同步方法)
        
        Args:
            task_id (str): 任务 ID
            use_last_result (bool): 是否使用缓存的最新结果，默认为 False
        
        Returns:
            Optional[TaskResult]: 任务结果，如果任务不存在或未完成，则返回 None
        """
        group_id: Optional[str] = next((group_id for group_id, task_id_in_group in self.task_groups.items() if task_id_in_group == task_id), None)
        if not group_id or self.task_group_status.get(group_id) != TaskGroupStatus.ACTIVE:
            logging.warning(f"Task {task_id} not found in any active group")
            return None

        if use_last_result and task_id in self.last_results:
            logging.info(f"Returning cached last result for task {task_id}")
            return self.last_results[task_id]
        
        if group_id not in self.result_queues:
            logging.warning(f"Result queue for group {group_id} not found")
            return None

        result_queue: deque[TaskResult] = self.result_queues[group_id]
        
        if result_queue:
            result: TaskResult = result_queue.popleft()
            if result.task_id == task_id:
                if self.cache_last_result:
                    self.last_results[task_id] = result  # 缓存最新结果
                return result
        return None

    async def get_results_generator(self, task_id: str, use_last_result: bool = False) -> AsyncGenerator[TaskResult, None]:
        """
        异步生成器，获取指定任务的所有结果
        
        Args:
            task_id (str): 任务 ID
            use_last_result (bool): 是否使用缓存的最新结果，默认为 False
        
        Yields:
            TaskResult: 任务结果
        """
        group_id: Optional[str] = next((group_id for group_id, task_id_in_group in self.task_groups.items() if task_id_in_group == task_id), None)
        if not group_id or self.task_group_status.get(group_id) != TaskGroupStatus.ACTIVE:
            logging.warning(f"Task {task_id} not found in any active group")
            return

        if use_last_result and task_id in self.last_results:
            logging.info(f"Yielding cached last result for task {task_id}")
            yield self.last_results[task_id]

        if group_id not in self.result_queues:
            logging.warning(f"Result queue for group {group_id} not found")
            return

        result_queue: deque[TaskResult] = self.result_queues[group_id]
        
        while True:
            if result_queue:
                result: TaskResult = result_queue.popleft()
                if result.task_id == task_id:
                    if self.cache_last_result:
                        self.last_results[task_id] = result  # 缓存最新结果
                    yield result
            else:
                await asyncio.sleep(0.1)  # 避免CPU占用过高

            # 检查任务是否完成
            if task_id not in self.running_tasks and task_id not in self.task_groups.values():
                break
        
        logging.info(f"Task {task_id} generator finished")

    async def wait_for_completion(self, group_id: str) -> None:
        """
        等待任务组完成
        
        Args:
            group_id (str): 任务组 ID
        """
        if group_id not in self.task_completion_events:
            raise ValueError(f"Task group {group_id} not found")
        await self.task_completion_events[group_id].wait()
        logging.info(f"Task group {group_id} has completed.")

    def get_metrics(self) -> Dict[str, Any]:
        """
        获取调度器监控信息
        
        Returns:
            Dict[str, Any]: 调度器监控信息
        """
        return {
            "total_tasks": self.total_tasks,
            "running_tasks": len(self.running_tasks),
            "queued_tasks": self.task_queue.qsize(),
            "max_workers": self.max_workers,
            "scheduler_running": self.scheduler_running,
        }

    async def _process_group_results(self, group_id: str) -> None:
        """
        处理任务组结果
        
        Args:
            group_id (str): 任务组 ID
        """
        if self.result_handler:
            results: List[TaskResult] = list(self.result_queues[group_id])  # 复制结果队列
            try:
                await self.result_handler.process_results(group_id, results)
            except Exception as e:
                logging.error(f"Error processing group results for {group_id}: {e}")
        else:
            logging.warning(f"No result handler configured for group {group_id}")

    """
    调度器 group_id, task_id 对应关系：
    
    - group_id: 任务组 ID，用于将多个任务组织在一起。
    - task_id: 任务 ID，作为协程任务的 key。
    
    task_groups 字典存储 group_id 和 task_id 的映射关系：
    {
        group_id: task_id,
        ...
    }
    """
