import time

from core.workflow.entities.variable_pool import VariablePool
from core.workflow.graph_engine.entities.event import GraphRunStartedEvent, GraphRunSucceededEvent
from core.workflow.graph_engine.entities.graph import Graph
from core.workflow.graph_engine.entities.graph_runtime_state import GraphRuntimeState
from core.workflow.graph_engine.queue_based_graph_engine import QueueBasedGraphEngine
from core.workflow.system_variable import SystemVariable


class TestQueueBasedGraphEngineConditionalNodes:
    def test_if_else_node_true_branch(self, app):
        """
        Test queue-based engine with if-else node taking the true branch.

        Graph structure:
        start -> if-else -> success_node
                     |
                     -> failure_node

        Tests:
        1. If-else condition evaluation
        2. Correct branch selection based on condition result
        3. Variable passing through conditional logic
        """
        # Arrange - Workflow with if-else branching
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
                                "max_length": 100,
                                "options": [],
                                "required": True,
                                "type": "text-input",
                                "variable": "query",
                            },
                            {
                                "label": "success_message",
                                "max_length": 100,
                                "options": [],
                                "required": True,
                                "type": "text-input",
                                "variable": "success_message",
                            },
                            {
                                "label": "failure_message",
                                "max_length": 100,
                                "options": [],
                                "required": True,
                                "type": "text-input",
                                "variable": "failure_message",
                            },
                        ],
                    },
                },
                {
                    "id": "if-else-1",
                    "data": {
                        "type": "if-else",
                        "title": "Check Query",
                        "cases": [
                            {
                                "case_id": "true",
                                "logical_operator": "and",
                                "conditions": [
                                    {
                                        "comparison_operator": "contains",
                                        "variable_selector": ["sys", "query"],
                                        "value": "hello",
                                    }
                                ],
                            }
                        ],
                    },
                },
                {
                    "id": "success",
                    "data": {
                        "type": "end",
                        "title": "Success Path",
                        "outputs": [{"value_selector": ["start", "success_message"], "variable": "result"}],
                    },
                },
                {
                    "id": "failure",
                    "data": {
                        "type": "end",
                        "title": "Failure Path",
                        "outputs": [{"value_selector": ["start", "failure_message"], "variable": "result"}],
                    },
                },
            ],
            "edges": [
                {"id": "e1", "source": "start", "target": "if-else-1"},
                {"id": "e2", "source": "if-else-1", "sourceHandle": "true", "target": "success"},
                {"id": "e3", "source": "if-else-1", "sourceHandle": "false", "target": "failure"},
            ],
        }

        # Create graph and runtime state with query containing "hello"
        graph = Graph.init(graph_config=graph_config)
        variable_pool = VariablePool(
            system_variables=SystemVariable(
                user_id="test_user",
                app_id="test_app",
                workflow_id="test_workflow",
                files=[],
                query="hello world",  # This should match the condition
            ),
            user_inputs={
                "query": "hello world",
                "success_message": "SUCCESS_PATH_TAKEN",
                "failure_message": "FAILURE_PATH_TAKEN",
            },
        )

        runtime_state = GraphRuntimeState(variable_pool=variable_pool, start_at=time.perf_counter())

        # Act
        engine = QueueBasedGraphEngine(
            graph=graph,
            runtime_state=runtime_state,
            max_execution_steps=100,
            max_execution_time=30,
            worker_count=1,
        )

        events = list(engine.run())

        # Assert
        # Should execute: start -> if-else -> success
        assert len(events) >= 4  # GraphRunStarted, RunCompleted events, GraphRunSucceeded
        assert isinstance(events[0], GraphRunStartedEvent)
        assert isinstance(events[-1], GraphRunSucceededEvent)

        # Verify the success path was taken
        final_outputs = events[-1].outputs
        assert final_outputs is not None
        assert "result" in final_outputs
        assert final_outputs["result"] == "SUCCESS_PATH_TAKEN"  # Should come from success node

        # Verify the correct nodes were executed (start, if-else, success)
        assert len(engine.completed_nodes) == 3
        assert "start" in engine.completed_nodes
        assert "if-else-1" in engine.completed_nodes
        assert "success" in engine.completed_nodes
        assert "failure" not in engine.completed_nodes  # Failure node should not execute

    def test_if_else_node_false_branch(self, app):
        """
        Test queue-based engine with if-else node taking the false branch.

        Tests the same graph structure but with input that fails the condition.
        """
        # Arrange - Same graph structure as above
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
                                "max_length": 100,
                                "options": [],
                                "required": True,
                                "type": "text-input",
                                "variable": "query",
                            },
                            {
                                "label": "success_message",
                                "max_length": 100,
                                "options": [],
                                "required": True,
                                "type": "text-input",
                                "variable": "success_message",
                            },
                            {
                                "label": "failure_message",
                                "max_length": 100,
                                "options": [],
                                "required": True,
                                "type": "text-input",
                                "variable": "failure_message",
                            },
                        ],
                    },
                },
                {
                    "id": "if-else-1",
                    "data": {
                        "type": "if-else",
                        "title": "Check Query",
                        "cases": [
                            {
                                "case_id": "true",
                                "logical_operator": "and",
                                "conditions": [
                                    {
                                        "comparison_operator": "contains",
                                        "variable_selector": ["sys", "query"],
                                        "value": "hello",
                                    }
                                ],
                            }
                        ],
                    },
                },
                {
                    "id": "success",
                    "data": {
                        "type": "end",
                        "title": "Success Path",
                        "outputs": [{"value_selector": ["start", "success_message"], "variable": "result"}],
                    },
                },
                {
                    "id": "failure",
                    "data": {
                        "type": "end",
                        "title": "Failure Path",
                        "outputs": [{"value_selector": ["start", "failure_message"], "variable": "result"}],
                    },
                },
            ],
            "edges": [
                {"id": "e1", "source": "start", "target": "if-else-1"},
                {"id": "e2", "source": "if-else-1", "sourceHandle": "true", "target": "success"},
                {"id": "e3", "source": "if-else-1", "sourceHandle": "false", "target": "failure"},
            ],
        }

        # Create graph and runtime state with query NOT containing "hello"
        graph = Graph.init(graph_config=graph_config)
        variable_pool = VariablePool(
            system_variables=SystemVariable(
                user_id="test_user",
                app_id="test_app",
                workflow_id="test_workflow",
                files=[],
                query="goodbye world",  # This should NOT match the condition
            ),
            user_inputs={
                "query": "goodbye world",
                "success_message": "SUCCESS_PATH_TAKEN",
                "failure_message": "FAILURE_PATH_TAKEN",
            },
        )

        runtime_state = GraphRuntimeState(variable_pool=variable_pool, start_at=time.perf_counter())

        # Act
        engine = QueueBasedGraphEngine(
            graph=graph,
            runtime_state=runtime_state,
            max_execution_steps=100,
            max_execution_time=30,
            worker_count=1,
        )

        events = list(engine.run())

        # Assert
        # Should execute: start -> if-else -> failure
        assert len(events) >= 4  # GraphRunStarted, RunCompleted events, GraphRunSucceeded
        assert isinstance(events[0], GraphRunStartedEvent)
        assert isinstance(events[-1], GraphRunSucceededEvent)

        # Verify the failure path was taken
        final_outputs = events[-1].outputs
        assert final_outputs is not None
        assert "result" in final_outputs
        assert final_outputs["result"] == "FAILURE_PATH_TAKEN"  # Should come from failure node

        # Verify the correct nodes were executed (start, if-else, failure)
        assert len(engine.completed_nodes) == 3
        assert "start" in engine.completed_nodes
        assert "if-else-1" in engine.completed_nodes
        assert "failure" in engine.completed_nodes
        assert "success" not in engine.completed_nodes  # Success node should not execute

    def test_if_else_node_multiple_conditions_and_operator(self, app):
        """
        Test if-else node with multiple conditions using AND operator.

        Both conditions must be true for the true branch to execute.
        """
        # Arrange - If-else with multiple AND conditions
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
                                "max_length": 100,
                                "options": [],
                                "required": True,
                                "type": "text-input",
                                "variable": "query",
                            },
                            {
                                "label": "score",
                                "max_length": 10,
                                "options": [],
                                "required": True,
                                "type": "number",
                                "variable": "score",
                            },
                            {
                                "label": "premium_message",
                                "max_length": 100,
                                "options": [],
                                "required": True,
                                "type": "text-input",
                                "variable": "premium_message",
                            },
                            {
                                "label": "standard_message",
                                "max_length": 100,
                                "options": [],
                                "required": True,
                                "type": "text-input",
                                "variable": "standard_message",
                            },
                        ],
                    },
                },
                {
                    "id": "if-else-1",
                    "data": {
                        "type": "if-else",
                        "title": "Check Query and Score",
                        "cases": [
                            {
                                "case_id": "both_conditions_met",
                                "logical_operator": "and",
                                "conditions": [
                                    {
                                        "comparison_operator": "contains",
                                        "variable_selector": ["sys", "query"],
                                        "value": "premium",
                                    },
                                    {
                                        "comparison_operator": "≥",
                                        "variable_selector": ["start", "score"],
                                        "value": "80",
                                    },
                                ],
                            }
                        ],
                    },
                },
                {
                    "id": "premium",
                    "data": {
                        "type": "end",
                        "title": "Premium User",
                        "outputs": [{"value_selector": ["start", "premium_message"], "variable": "user_type"}],
                    },
                },
                {
                    "id": "standard",
                    "data": {
                        "type": "end",
                        "title": "Standard User",
                        "outputs": [{"value_selector": ["start", "standard_message"], "variable": "user_type"}],
                    },
                },
            ],
            "edges": [
                {"id": "e1", "source": "start", "target": "if-else-1"},
                {"id": "e2", "source": "if-else-1", "sourceHandle": "both_conditions_met", "target": "premium"},
                {"id": "e3", "source": "if-else-1", "sourceHandle": "false", "target": "standard"},
            ],
        }

        # Test case 1: Both conditions are true (should take premium path)
        graph = Graph.init(graph_config=graph_config)
        variable_pool = VariablePool(
            system_variables=SystemVariable(
                user_id="test_user",
                app_id="test_app",
                workflow_id="test_workflow",
                files=[],
                query="premium account",  # Contains "premium"
            ),
            user_inputs={
                "query": "premium account",
                "score": 85,
                "premium_message": "PREMIUM_USER_DETECTED",
                "standard_message": "STANDARD_USER_DETECTED",
            },
        )

        runtime_state = GraphRuntimeState(variable_pool=variable_pool, start_at=time.perf_counter())

        engine = QueueBasedGraphEngine(
            graph=graph,
            runtime_state=runtime_state,
            max_execution_steps=100,
            max_execution_time=30,
            worker_count=1,
        )

        events = list(engine.run())

        # Verify premium path was taken
        final_outputs = events[-1].outputs
        assert final_outputs["user_type"] == "PREMIUM_USER_DETECTED"  # Contains the premium message
        assert "premium" in engine.completed_nodes
        assert "standard" not in engine.completed_nodes

    def test_if_else_node_multiple_conditions_or_operator(self, app):
        """
        Test if-else node with multiple conditions using OR operator.

        Only one condition needs to be true for the true branch to execute.
        """
        # Arrange - If-else with multiple OR conditions
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
                                "max_length": 100,
                                "options": [],
                                "required": True,
                                "type": "text-input",
                                "variable": "query",
                            },
                            {
                                "label": "high_priority_message",
                                "max_length": 100,
                                "options": [],
                                "required": True,
                                "type": "text-input",
                                "variable": "high_priority_message",
                            },
                            {
                                "label": "normal_priority_message",
                                "max_length": 100,
                                "options": [],
                                "required": True,
                                "type": "text-input",
                                "variable": "normal_priority_message",
                            },
                        ],
                    },
                },
                {
                    "id": "if-else-1",
                    "data": {
                        "type": "if-else",
                        "title": "Check Query Keywords",
                        "cases": [
                            {
                                "case_id": "urgent_request",
                                "logical_operator": "or",
                                "conditions": [
                                    {
                                        "comparison_operator": "contains",
                                        "variable_selector": ["sys", "query"],
                                        "value": "urgent",
                                    },
                                    {
                                        "comparison_operator": "contains",
                                        "variable_selector": ["sys", "query"],
                                        "value": "emergency",
                                    },
                                    {
                                        "comparison_operator": "contains",
                                        "variable_selector": ["sys", "query"],
                                        "value": "asap",
                                    },
                                ],
                            }
                        ],
                    },
                },
                {
                    "id": "high_priority",
                    "data": {
                        "type": "end",
                        "title": "High Priority",
                        "outputs": [{"value_selector": ["start", "high_priority_message"], "variable": "priority"}],
                    },
                },
                {
                    "id": "normal_priority",
                    "data": {
                        "type": "end",
                        "title": "Normal Priority",
                        "outputs": [{"value_selector": ["start", "normal_priority_message"], "variable": "priority"}],
                    },
                },
            ],
            "edges": [
                {"id": "e1", "source": "start", "target": "if-else-1"},
                {"id": "e2", "source": "if-else-1", "sourceHandle": "urgent_request", "target": "high_priority"},
                {"id": "e3", "source": "if-else-1", "sourceHandle": "false", "target": "normal_priority"},
            ],
        }

        # Test case: Only one condition is true (should still take high priority path)
        graph = Graph.init(graph_config=graph_config)
        variable_pool = VariablePool(
            system_variables=SystemVariable(
                user_id="test_user",
                app_id="test_app",
                workflow_id="test_workflow",
                files=[],
                query="please help me asap with this issue",  # Contains "asap" but not "urgent" or "emergency"
            ),
            user_inputs={
                "query": "please help me asap with this issue",
                "high_priority_message": "HIGH_PRIORITY_REQUEST",
                "normal_priority_message": "NORMAL_PRIORITY_REQUEST",
            },
        )

        runtime_state = GraphRuntimeState(variable_pool=variable_pool, start_at=time.perf_counter())

        engine = QueueBasedGraphEngine(
            graph=graph,
            runtime_state=runtime_state,
            max_execution_steps=100,
            max_execution_time=30,
            worker_count=1,
        )

        events = list(engine.run())

        # Verify high priority path was taken (OR condition satisfied)
        final_outputs = events[-1].outputs
        # Contains the high priority message from high priority path
        assert final_outputs["priority"] == "HIGH_PRIORITY_REQUEST"
        assert "high_priority" in engine.completed_nodes
        assert "normal_priority" not in engine.completed_nodes
