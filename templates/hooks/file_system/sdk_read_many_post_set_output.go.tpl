	restoreNonRoundTrippedSpecFields(r, ko)
	recoverObservedLustreDataRepository(r, resp, ko)
	recoverObservedWindowsAliases(r, resp, ko)
	if err := terminalIfFailed(ko); err != nil {
		return &resource{ko}, err
	}
