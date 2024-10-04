# Copyright (c) Microsoft Corporation.


if __name__ == "__main__":
    
    from bs4 import BeautifulSoup

    # Sample HTML content
    file = 'failed-3.1-run-2.html'
    html_content = open(file)

    # Parse the HTML with BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find all <span> tags
    spans = soup.find_all('span')

    # Extract the text from each <span> tag
    span_texts = [span.get_text() for span in spans]

    # Print the list of texts
    
    indexes = []
    for text in span_texts:
        try:
            index = int(text)
        except:
            index = -1
        
        if index >= 0:
            indexes.append(index)
    
    indexes = sorted(list(set(indexes)))
    
    print(indexes)
    print(len(indexes))
