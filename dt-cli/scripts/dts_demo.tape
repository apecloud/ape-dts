# vhs < dts_demo.tape

# Canvas settings
Output demo.gif
Set FontSize 14
Set Width 1200
Set Height 650
Set Theme "Dracula"
Set TypingSpeed 50ms

# Opening: show prompt, wait a moment
Sleep 500ms

# Type command (like a human typing)
Type "dtscli --help"
Sleep 200ms
Enter
Sleep 1s

# Preflight
Enter
Type "dtscli create --mode cdc --name test1 --source mysql://root:123456@127.0.0.1:3306 --target mysql://root:123456@127.0.0.1:3307 --do test_db1 --preflight"
Sleep 200ms
Enter
Sleep 3s

# Create demo
Enter
Type "dtscli create --mode cdc --name test1 --source mysql://root:123456@127.0.0.1:3306 --target mysql://root:123456@127.0.0.1:3307 --do test_db1"
Sleep 200ms
Enter
Sleep 3s

# Show list result
Enter
Type "dtscli list"
Enter
Sleep 1.5s

# Show logs result
Enter
Type "dtscli logs test1"
Enter
Sleep 1.5s
