package com.findwise.hydra.stage;

/**
 * Kills stages by exiting the JVM.
 *
 * Will unregister shutdown hooks.
 */
public class JvmStageKiller implements StageKiller {
	@Override
	public void kill(AbstractStage stage) {
		Thread shutdownHook = stage.getShutDownHook();
		if (null != shutdownHook) {
			Runtime.getRuntime().removeShutdownHook(shutdownHook);
		}
		System.exit(1);
	}
}
