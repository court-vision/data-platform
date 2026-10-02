// Who is saving adjustments from this browser. The dashboard signs in with a
// shared token, so the name on a version is whatever the person typed last.
const AUTHOR_KEY = "cv_projection_author"
export const DEFAULT_AUTHOR = "dashboard"

export function readAuthor(): string {
  try {
    return localStorage.getItem(AUTHOR_KEY)?.trim() || DEFAULT_AUTHOR
  } catch {
    return DEFAULT_AUTHOR
  }
}

export function rememberAuthor(author: string) {
  try {
    localStorage.setItem(AUTHOR_KEY, author.trim() || DEFAULT_AUTHOR)
  } catch {
    // storage refused: the name lasts as long as the form does
  }
}
