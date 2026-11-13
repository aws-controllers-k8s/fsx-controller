	// FSx appends "/" to a DataRepositoryPath given without one; keep the
	// user's spelling so the immutable field is not rewritten.
	keepDataRepositoryPathSpelling(r.ko, ko)
	// FAILED is documented as unrecoverable; surface it as Terminal with
	// FSx's FailureDetails rather than requeueing forever.
	if err := terminalIfFailed(ko); err != nil {
		return &resource{ko}, err
	}
