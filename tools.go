package goscript

import "context"

// ReadFile is a convenience method that creates a new script instance to read a file
// as a blob and return its contents as a string.
//
// Parameters:
//   - ctx: Context for execution control and cancellation
//   - path: File path to read
//
// Returns:
//   - string: The file contents as a string
//   - error: Any error that occurred during file reading
//
// Example:
//
//	content, err := goscript.ReadFile(ctx, "config.txt")
func ReadFile(ctx context.Context, path string) (string, error) {
	ss := New() // new script instance to avoid interference

	file, err := ss.BlobFileIn(path).ToString(ctx)
	if err != nil {
		return "", err
	}

	return file, nil
}
