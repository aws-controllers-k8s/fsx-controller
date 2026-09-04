	restoreNonRoundTrippedSpecFields(r, ko)
	recoverObservedLustreDataRepository(r, resp, ko)
	ensureLastAppliedSecretBaseline(ko)
	ensureImmutableFieldBaseline(ko)
