namespace Storage.Net
{
   /// <summary>
   /// Generic error codes for storage operations
   /// </summary>
   public enum ErrorCode
   {
      /// <summary>
      /// Unknown error code.
      /// </summary>
      Unknown,

      /// <summary>
      /// Resource not found.
      /// </summary>
      NotFound,

      /// <summary>
      /// Operation failed because a key already exists.
      /// </summary>
      DuplicateKey,

      /// <summary>
      /// Failed the precondition.
      /// </summary>
      PreconditionFailed,

      /// <summary>
      /// A conflict.
      /// </summary>
      Conflict,

      /// <summary>
      /// A bad request.
      /// </summary>
      BadRequest
   }
}
