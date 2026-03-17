"""Tests for the pipeline core: Stage, Pipeline, and PipelineContext."""

import pytest

from src.pipeline.queue import Pipeline, PipelineContext, Stage


class PassThroughStage(Stage):
    """A stage that does nothing — used for testing."""

    def process(self, context: PipelineContext) -> PipelineContext:
        return context


class AddErrorStage(Stage):
    """A stage that adds a warning to the context."""

    def process(self, context: PipelineContext) -> PipelineContext:
        context.errors.append("test warning")
        return context


class FailingStage(Stage):
    """A stage that raises an exception."""

    def process(self, context: PipelineContext) -> PipelineContext:
        raise ValueError("stage exploded")


class TestPipelineContext:
    def test_defaults(self):
        ctx = PipelineContext(file_path="test.pdf")
        assert ctx.file_path == "test.pdf"
        assert ctx.raw_header == {}
        assert ctx.raw_lines == []
        assert ctx.statement_id is None
        assert ctx.classified_lines == []
        assert ctx.unclassified_lines == []
        assert ctx.errors == []

    def test_has_errors(self):
        ctx = PipelineContext(file_path="test.pdf")
        assert not ctx.has_errors
        ctx.errors.append("something")
        assert ctx.has_errors

    def test_summary(self):
        ctx = PipelineContext(file_path="test.pdf")
        ctx.classified_lines = [{"id": 1}, {"id": 2}]
        ctx.unclassified_lines = [{"id": 3}]
        summary = ctx.summary()
        assert summary["total_lines"] == 3
        assert summary["classified"] == 2
        assert summary["unclassified"] == 1


class TestPipeline:
    def test_empty_pipeline(self):
        pipeline = Pipeline(stages=[])
        ctx = PipelineContext(file_path="test.pdf")
        result = pipeline.run(ctx)
        assert result.file_path == "test.pdf"

    def test_passthrough_stage(self):
        pipeline = Pipeline(stages=[PassThroughStage()])
        ctx = PipelineContext(file_path="test.pdf")
        result = pipeline.run(ctx)
        assert result.file_path == "test.pdf"

    def test_multiple_stages(self):
        pipeline = Pipeline(stages=[PassThroughStage(), AddErrorStage()])
        ctx = PipelineContext(file_path="test.pdf")
        result = pipeline.run(ctx)
        assert len(result.errors) == 1
        assert result.errors[0] == "test warning"

    def test_failing_stage_raises(self):
        pipeline = Pipeline(stages=[FailingStage()])
        ctx = PipelineContext(file_path="test.pdf")
        with pytest.raises(ValueError, match="stage exploded"):
            pipeline.run(ctx)

    def test_stage_name(self):
        stage = PassThroughStage()
        assert stage.name == "PassThroughStage"
