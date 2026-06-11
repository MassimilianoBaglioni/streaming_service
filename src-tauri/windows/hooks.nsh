!macro NSIS_HOOK_PREINSTALL
  # Creates a directory where all the dlls and the exe is placed, to avoid flooding the user's target directory. 
  
  Push $0
  Push $1

  StrLen $0 "\streaming_server"
  
  IntOp $0 0 - $0 
  
  StrCpy $1 $INSTDIR "" $0
  
  ${If} $1 != "\streaming_server"
    StrCpy $INSTDIR "$INSTDIR\streaming_server"
  ${EndIf}

  Pop $1
  Pop $0
!macroend