package com.findwise.hydra.memorydb;

import org.junit.Ignore;
import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

public class RepeatRunner extends BlockJUnit4ClassRunner {

	public RepeatRunner(Class<?> clazz) throws InitializationError {
		super(clazz);
	}

	@Override
	public Description describeChild(FrameworkMethod method) {
		if (isRepeated(method)) {
			int times = method.getAnnotation(Repeat.class).value();

			Description description = Description.createSuiteDescription(
					testName(method) + " [" + times + " times]",
					method.getAnnotations());

			for (int i = 1; i <= times; i++) {
				description.addChild(Description.createTestDescription(
						getTestClass().getJavaClass(), "[" + i + "] "
								+ testName(method)));
			}
			return description;
		}
		return super.describeChild(method);
	}

	private boolean isRepeated(FrameworkMethod method) {
		return method.getAnnotation(Repeat.class) != null
				&& method.getAnnotation(Ignore.class) == null;
	}

	@Override
	protected void runChild(FrameworkMethod method, RunNotifier notifier) {
		if (isRepeated(method)) {
			Description description = describeChild(method);

			for (Description desc : description.getChildren()) {
				runLeaf(methodBlock(method), desc, notifier);
			}
		} else {
			super.runChild(method, notifier);
		}
	}
}
