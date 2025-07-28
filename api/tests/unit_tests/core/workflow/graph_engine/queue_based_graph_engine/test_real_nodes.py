import time

from core.workflow.entities.variable_pool import VariablePool
from core.workflow.graph_engine.entities.event import GraphRunStartedEvent, GraphRunSucceededEvent
from core.workflow.graph_engine.entities.graph import Graph
from core.workflow.graph_engine.entities.graph_runtime_state import GraphRuntimeState
from core.workflow.graph_engine.queue_based_graph_engine import QueueBasedGraphEngine
from core.workflow.system_variable import SystemVariable


class TestQueueBasedGraphEngineRealNodes:
    def test_simple_start_end_workflow_with_real_nodes(self, app):
        """
        Test queue-based engine with a simple start->end workflow using real node implementations.

        This test verifies:
        1. Real start and end nodes can be executed
        2. Queue-based engine works without mocking
        3. Proper variable passing between nodes
        4. Correct event emission from real nodes
        """
        # Arrange - Simple start->end workflow
        graph_config = {
            "nodes": [
                {
                    "id": "start",
                    "data": {
                        "type": "start",
                        "title": "Start",
                        "variables": [
                            {
                                "label": "query",
                                "max_length": 48,
                                "options": [],
                                "required": True,
                                "type": "text-input",
                                "variable": "query",
                            }
                        ],
                    },
                },
                {
                    "id": "end",
                    "data": {
                        "type": "end",
                        "title": "End",
                        "outputs": [{"value_selector": ["sys", "query"], "variable": "result"}],
                    },
                },
            ],
            "edges": [{"id": "e1", "source": "start", "target": "end"}],
        }

        # Create graph and runtime state
        graph = Graph.init(graph_config=graph_config)
        variable_pool = VariablePool(
            system_variables=SystemVariable(
                user_id="test_user", app_id="test_app", workflow_id="test_workflow", files=[], query="Hello World"
            ),
            user_inputs={"query": "Hello World"},
        )

        runtime_state = GraphRuntimeState(variable_pool=variable_pool, start_at=time.perf_counter())

        # Act - Use QueueBasedGraphEngine without node_executor (uses real nodes)
        engine = QueueBasedGraphEngine(
            graph=graph,
            runtime_state=runtime_state,
            max_execution_steps=100,
            max_execution_time=30,
            worker_count=1,  # Use single worker for predictable execution order
        )

        events = list(engine.run())

        # Assert
        # Verify we got exactly 4 events: GraphRunStarted, RunCompleted (start), RunCompleted (end), GraphRunSucceeded
        assert len(events) == 4
        assert isinstance(events[0], GraphRunStartedEvent)
        assert isinstance(events[-1], GraphRunSucceededEvent)

        # Verify the output contains our query
        final_outputs = events[-1].outputs
        assert final_outputs is not None
        assert "result" in final_outputs
        assert final_outputs["result"] == "Hello World"

        # Verify engine completed successfully
        assert engine.pending_tasks == 0
        assert len(engine.completed_nodes) == 2  # start and end nodes completed
