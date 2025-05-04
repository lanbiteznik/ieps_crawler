from bs4 import BeautifulSoup
from typing import Tuple, Optional, List

class HTMLCleaner:
    PARAGRAPH_BREAK = "\n<<<PARAGRAPH>>>\n"
    SUB_PARAGRAPH_BREAK = "\n<<<SUB>>>\n"

    @staticmethod
    def split_subparagraphs(block) -> List[str]:
        """Split larger <div> blocks into smaller units.
        
        Args:
            block: BeautifulSoup element to process
            
        Returns:
            List of text segments from the block
        """
        subunits = []
        children = block.find_all(
            ["p", "span", "br", "li", "h1", "h2", "h3", "h4"], 
            recursive=False
        )

        for child in children:
            text = child.get_text(separator=" ", strip=True)
            if text:
                subunits.append(text)

        if not subunits:
            whole = block.get_text(separator=" ", strip=True)
            if whole:
                subunits.append(whole)

        return subunits

    @classmethod
    def clean_html(cls, html: str) -> Tuple[Optional[str], bool]:
        """Clean HTML content and extract meaningful text.
        
        Args:
            html: Raw HTML content to clean
            
        Returns:
            Tuple of (cleaned text, whether block-system was used)
        """
        if not html:
            return None, False

        soup = BeautifulSoup(html, "html.parser")
        
        breadcrumbs_text = None
        breadcrumbs = soup.select_one(".breadcrumbs")
        if breadcrumbs:
            breadcrumbs_text = breadcrumbs.get_text(separator=" ", strip=True)
            breadcrumbs.decompose()

        content = soup.select_one(".block.block-system")
        used_block = bool(content)
        content = content if content else soup

        for tag in content(["script", "style", "header", "footer", "nav"]):
            tag.decompose()

        blocks = content.find_all(
            ["p", "div", "section", "article", "li", "h1", "h2", "h3", "h4"]
        )
        
        paragraphs = []
        for block in blocks:
            if block.get_text(strip=True):
                sub_parts = cls.split_subparagraphs(block)
                combined = cls.SUB_PARAGRAPH_BREAK.join(sub_parts)
                paragraphs.append(combined)

        paragraphs = list(dict.fromkeys(paragraphs))

        if breadcrumbs_text:
            paragraphs.insert(0, breadcrumbs_text)

        plain_text = cls.PARAGRAPH_BREAK.join(paragraphs).strip()

        if not plain_text.strip():
            return None, used_block
        text_lower = plain_text.lower()
        if any(phrase in text_lower for phrase in [
            "page does not exist",
            "zavrnjen dostop",
            "page does not exsist!",
            "pdf"
        ]):
            return None, used_block

        return plain_text, used_block
    

if __name__ == "__main__":
    html_content = """
    <html>
      <head><title>Sample Page</title></head>
      <body>
        <div class='header'>Welcome to the site!</div>
        <div class='block block-system'>
          <h1>Important Content</h1>
          <p>This is the main section that should be cleaned and extracted.</p>
        </div>
        <div class='footer'>Contact us at support@example.com</div>
      </body>
    </html>
    """
    cleaner = HTMLCleaner()
    cleaned_text, used_block = cleaner.clean_html(html_content)
    print("Cleaned Text:", cleaned_text)
    print("Used Block System:", used_block)
