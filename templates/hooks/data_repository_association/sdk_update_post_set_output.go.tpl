	// Tags are not part of UpdateDataRepositoryAssociation; they were synced
	// with TagResource/UntagResource above, so the Update response is not
	// authoritative for them. Keep the desired tags.
	ko.Spec.Tags = desired.ko.Spec.Tags
	keepDataRepositoryPathSpelling(desired.ko, ko)
