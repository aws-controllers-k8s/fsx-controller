	// FSx reorders the OpenZFS client configurations and quotas and always
	// reports the root quotas; don't treat that as a change.
	normalizeOpenZFSLists(a, b)
