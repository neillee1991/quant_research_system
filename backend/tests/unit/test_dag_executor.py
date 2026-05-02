"""DAGExecutor.topological_sort 单元测试"""
import pytest
from scheduler.executor import DAGExecutor


def make_task(task_id: str, depends_on: list[str] | None = None) -> dict:
    return {"id": task_id, "type": "sync", "depends_on": depends_on or []}


class TestTopologicalSort:

    def test_single_task(self):
        tasks = [make_task("a")]
        layers = DAGExecutor.topological_sort(tasks)
        assert layers == [["a"]]

    def test_linear_chain(self):
        # a -> b -> c
        tasks = [
            make_task("a"),
            make_task("b", ["a"]),
            make_task("c", ["b"]),
        ]
        layers = DAGExecutor.topological_sort(tasks)
        assert layers == [["a"], ["b"], ["c"]]

    def test_parallel_tasks(self):
        # a, b 无依赖，c 依赖 a 和 b
        tasks = [
            make_task("a"),
            make_task("b"),
            make_task("c", ["a", "b"]),
        ]
        layers = DAGExecutor.topological_sort(tasks)
        assert layers[0] == sorted(layers[0])  # 第一层包含 a, b
        assert set(layers[0]) == {"a", "b"}
        assert layers[1] == ["c"]

    def test_diamond_shape(self):
        # a -> b, a -> c, b -> d, c -> d
        tasks = [
            make_task("a"),
            make_task("b", ["a"]),
            make_task("c", ["a"]),
            make_task("d", ["b", "c"]),
        ]
        layers = DAGExecutor.topological_sort(tasks)
        assert layers[0] == ["a"]
        assert set(layers[1]) == {"b", "c"}
        assert layers[2] == ["d"]

    def test_cycle_raises(self):
        # a -> b -> a 循环
        tasks = [
            make_task("a", ["b"]),
            make_task("b", ["a"]),
        ]
        with pytest.raises(ValueError, match="循环依赖"):
            DAGExecutor.topological_sort(tasks)

    def test_self_loop_raises(self):
        tasks = [make_task("a", ["a"])]
        with pytest.raises(ValueError, match="循环依赖"):
            DAGExecutor.topological_sort(tasks)

    def test_three_node_cycle_raises(self):
        tasks = [
            make_task("a", ["c"]),
            make_task("b", ["a"]),
            make_task("c", ["b"]),
        ]
        with pytest.raises(ValueError, match="循环依赖"):
            DAGExecutor.topological_sort(tasks)

    def test_unknown_dependency_ignored(self):
        # depends_on 引用不存在的任务，应忽略（不计入入度）
        tasks = [
            make_task("a", ["nonexistent"]),
            make_task("b", ["a"]),
        ]
        layers = DAGExecutor.topological_sort(tasks)
        assert layers[0] == ["a"]
        assert layers[1] == ["b"]

    def test_empty_tasks(self):
        layers = DAGExecutor.topological_sort([])
        assert layers == []

    def test_all_independent(self):
        tasks = [make_task("a"), make_task("b"), make_task("c")]
        layers = DAGExecutor.topological_sort(tasks)
        assert len(layers) == 1
        assert set(layers[0]) == {"a", "b", "c"}

    def test_layer_count_complex(self):
        # a -> c, b -> c, c -> d, c -> e
        tasks = [
            make_task("a"),
            make_task("b"),
            make_task("c", ["a", "b"]),
            make_task("d", ["c"]),
            make_task("e", ["c"]),
        ]
        layers = DAGExecutor.topological_sort(tasks)
        assert len(layers) == 3
        assert set(layers[0]) == {"a", "b"}
        assert layers[1] == ["c"]
        assert set(layers[2]) == {"d", "e"}
