package examples

//func main() {
//	fmt.Println("This is an example function.")
//
//	// Set slog to debug level
//	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
//		Level: slog.LevelDebug,
//	}))
//	slog.SetDefault(logger)
//
//	ctx := context.Background()
//
//	//textFormatting := pipeline.New().
//	//	Chain(routines.Transform(strings.ToUpper)).
//	//	Chain(routines.Transform(func(t string) string {
//	//		return strings.ReplaceAll(t, " ", "_")
//	//	}))
//
//	//script := goscript.New()
//	//str, err := script.
//	//	BlobFileIn("data/example.txt").
//	//	Chain(textFormatting).
//	//	ToString(ctx)
//
//	str, err := goscript.ReadFile(ctx, "data/example.txt")
//	if err != nil {
//		panic(err)
//	}
//
//	fmt.Println(str)
//	// Output:
//	// Sugarburgh
//	// Goldfield
//	// North Dayham
//	// Sweetwich
//	// Hapview
//	// Southwold
//	// Broken Shield
//	// Satbury
//	// Poltragow
//	// Brickelwhyte
//}
