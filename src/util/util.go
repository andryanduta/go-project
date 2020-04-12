package util

func InArrayString(value string, list []string) bool {
	for _, x := range list {
		if x == value {
			return true
		}
	}

	return false
}
