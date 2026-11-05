	// Delete is async: the association stays DELETING until FSx removes it, so
	// keep the finalizer. Later reconciles are held by the deletable.when guard
	// until Describe reports DataRepositoryAssociationNotFound.
	if err == nil {
		return r, requeueWaitWhileDeleting
	}
